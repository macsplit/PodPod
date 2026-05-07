const fs = require('fs');
const path = require('path');
const Parser = require('rss-parser');
const { default: PQueue } = require('p-queue');
const axios = require('axios');
const { opmlToJSON } = require('opml-to-json');
const db = require('./db');

const parser = new Parser();
const downloadQueue = new PQueue({ concurrency: 5 });

const STORAGE_ROOT = process.env.STORAGE_ROOT || '/disk/Pod';

// Track sync state for round-robin
let syncState = {
  feedIndex: 0,
  active: false,
  pauseUntil: 0
};

async function initFeedsFromOpml(opmlPath) {
  const opmlContent = fs.readFileSync(opmlPath, 'utf8');
  const json = await opmlToJSON(opmlContent);
  
  const feeds = [];
  function traverse(node) {
    if (node.xmlurl) {
      feeds.push({
        title: node.title || node.text,
        xmlUrl: node.xmlurl
      });
    }
    if (node.children) {
      node.children.forEach(traverse);
    }
  }
  traverse(json);

  const insertFeed = db.prepare('INSERT OR IGNORE INTO feeds (title, xml_url) VALUES (?, ?)');
  const transaction = db.transaction((feeds) => {
    for (const feed of feeds) {
      insertFeed.run(feed.title, feed.xmlUrl);
    }
  });
  transaction(feeds);
  return feeds.length;
}

// Background worker that cycles through feeds and processes them in small batches
async function archiveSyncWorker() {
  if (syncState.active) return;
  if (Date.now() < syncState.pauseUntil) return;

  syncState.active = true;
  const startTime = Date.now();
  const feeds = db.prepare('SELECT * FROM feeds').all();

  try {
    while (Date.now() - startTime < 30 * 60 * 1000) { // 30 min duty cycle
      if (syncState.feedIndex >= feeds.length) syncState.feedIndex = 0;
      const feed = feeds[syncState.feedIndex];
      
      try {
        console.log(`Archival sync: ${feed.title}`);
        const feedData = await parser.parseURL(feed.xml_url);
        
        // Find episodes not in DB
        const allEpisodes = feedData.items;
        allEpisodes.sort((a, b) => new Date(b.pubDate) - new Date(a.pubDate));
        
        // Find existing to skip
        for (const item of allEpisodes) {
           const guid = item.guid || item.link || item.enclosure?.url;
           if (!guid) continue;
           
           const existing = db.prepare('SELECT id FROM episodes WHERE guid = ?').get(guid);
           if (!existing) {
             const pubDate = item.pubDate ? new Date(item.pubDate).getTime() : Date.now();
             const enclosureUrl = item.enclosure?.url;
             if (!enclosureUrl) continue;

             db.prepare(`
               INSERT INTO episodes (feed_id, guid, title, link, pub_date, description, enclosure_url)
               VALUES (?, ?, ?, ?, ?, ?, ?)
             `).run(feed.id, guid, item.title, item.link, pubDate, item.contentSnippet || item.content, enclosureUrl);
             
             addToQueue(guid);
           }
        }
      } catch (err) {
        console.error(`Error syncing archive for ${feed.title}:`, err.message);
      }
      
      syncState.feedIndex++;
      // Small sleep to yield
      await new Promise(r => setTimeout(r, 1000));
    }
  } finally {
    syncState.pauseUntil = Date.now() + 5 * 60 * 1000; // 5 min pause
    syncState.active = false;
    console.log('Archival sync paused.');
  }
}

// Keep the lightweight check for new episodes
async function checkFeeds() {
  const feeds = db.prepare('SELECT * FROM feeds').all();
  for (const feed of feeds) {
    try {
      const feedData = await parser.parseURL(feed.xml_url);
      const recent = feedData.items.slice(0, 50);

      for (const item of recent) {
        const guid = item.guid || item.link || item.enclosure?.url;
        if (!guid) continue;

        const existing = db.prepare('SELECT id FROM episodes WHERE guid = ?').get(guid);
        if (!existing) {
          const pubDate = item.pubDate ? new Date(item.pubDate).getTime() : Date.now();
          const enclosureUrl = item.enclosure?.url;
          if (!enclosureUrl) continue;

          db.prepare(`
            INSERT INTO episodes (feed_id, guid, title, link, pub_date, description, enclosure_url)
            VALUES (?, ?, ?, ?, ?, ?, ?)
          `).run(feed.id, guid, item.title, item.link, pubDate, item.contentSnippet || item.content, enclosureUrl);
          
          addToQueue(guid);
        }
      }
      db.prepare('UPDATE feeds SET last_checked = ? WHERE id = ?').run(Date.now(), feed.id);
    } catch (error) {
      console.error(`Error checking feed ${feed.title}:`, error.message);
    }
  }
}

async function addToQueue(guid) {
  const episode = db.prepare(`
    SELECT e.*, f.title as feed_title 
    FROM episodes e 
    JOIN feeds f ON e.feed_id = f.id 
    WHERE e.guid = ?
  `).get(guid);

  if (!episode || episode.download_status === 'completed' || episode.download_status === 'downloading') return;

  downloadQueue.add(() => downloadEpisode(episode), { priority: episode.pub_date });
}

async function downloadEpisode(episode) {
  const feedDir = path.join(STORAGE_ROOT, episode.feed_title.replace(/[<>:"/\\|?*]/g, '_'));
  if (!fs.existsSync(feedDir)) {
    fs.mkdirSync(feedDir, { recursive: true });
  }

  const fileName = path.basename(new URL(episode.enclosure_url).pathname) || `${episode.id}.mp3`;
  const filePath = path.join(feedDir, fileName);

  db.prepare('UPDATE episodes SET download_status = ?, progress = 0, file_path = ? WHERE id = ?')
    .run('downloading', filePath, episode.id);

  try {
    const response = await axios({
      method: 'get',
      url: episode.enclosure_url,
      responseType: 'stream'
    });

    const totalLength = parseInt(response.headers['content-length'], 10);
    let downloadedLength = 0;

    const writer = fs.createWriteStream(filePath);
    
    response.data.on('data', (chunk) => {
      downloadedLength += chunk.length;
      if (totalLength) {
        const progress = Math.round((downloadedLength / totalLength) * 100);
        // Throttle DB updates for progress
        if (progress % 5 === 0) {
          db.prepare('UPDATE episodes SET progress = ? WHERE id = ?').run(progress, episode.id);
        }
      }
    });

    response.data.pipe(writer);

    return new Promise((resolve, reject) => {
      writer.on('finish', () => {
        db.prepare('UPDATE episodes SET download_status = ?, progress = 100 WHERE id = ?')
          .run('completed', episode.id);
        resolve();
      });
      writer.on('error', (err) => {
        fs.unlinkSync(filePath); // Clean up partial file
        db.prepare('UPDATE episodes SET download_status = ?, error_message = ? WHERE id = ?')
          .run('failed', err.message, episode.id);
        reject(err);
      });
    });

  } catch (error) {
    db.prepare('UPDATE episodes SET download_status = ?, error_message = ? WHERE id = ?')
      .run('failed', error.message, episode.id);
    throw error;
  }
}

// Start-up: queue all pending episodes
async function resumeDownloads() {
  const pending = db.prepare(`
    SELECT guid, pub_date, download_status FROM episodes 
    WHERE download_status = 'pending' OR download_status = 'downloading'
    ORDER BY pub_date DESC
  `).all();
  
  for (const ep of pending) {
    // If it was 'downloading', we reset it to 'pending' to restart
    if (ep.download_status === 'downloading') {
        db.prepare("UPDATE episodes SET download_status = 'pending', progress = 0 WHERE guid = ?").run(ep.guid);
    }
    addToQueue(ep.guid);
  }
}

module.exports = {
  initFeedsFromOpml,
  checkFeeds,
  archiveSyncWorker,
  resumeDownloads,
  downloadQueue
};
