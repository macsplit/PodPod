const fs = require('fs');
const path = require('path');
const { pipeline } = require('stream/promises');
const Parser = require('rss-parser');
const axios = require('axios');
const { default: PQueue } = require('p-queue');
const db = require('../db');

const parser = new Parser();
const downloadQueue = new PQueue({ concurrency: 5 });
const STORAGE_ROOT = process.env.STORAGE_ROOT || path.join(__dirname, '../data');

// Track sync state
let syncState = {
  feedIndex: 0,
  globalPauseUntil: 0,
  failedFeeds: new Set()
};

// Track attempts per episode
let downloadAttempts = new Map();

// Retry schedule in minutes
const RETRY_SCHEDULE_MINUTES = [5, 30, 240, 1440];

async function downloadEpisode(episode) {
  // Global holding pattern check
  if (Date.now() < syncState.globalPauseUntil) {
    console.log(`Global pause active. Re-queueing ${episode.title} in 1 hour.`);
    setTimeout(() => downloadQueue.add(() => downloadEpisode(episode)), 60 * 60 * 1000);
    return;
  }

  const attempts = downloadAttempts.get(episode.id) || 0;
  
  const feedDir = path.join(STORAGE_ROOT, (episode.feed_title || 'unknown').replace(/[<>:"/\\|?*]/g, '_'));
  if (!fs.existsSync(feedDir)) {
    fs.mkdirSync(feedDir, { recursive: true });
  }

  // Improved filename extraction
  const urlObj = new URL(episode.enclosure_url);
  const searchParams = new URLSearchParams(urlObj.search);
  const fileName = searchParams.get('filename') ? path.basename(searchParams.get('filename')) : (path.basename(urlObj.pathname) || `${episode.id}.mp3`);
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

    await pipeline(
      response.data,
      async function* (source) {
        for await (const chunk of source) {
          downloadedLength += chunk.length;
          if (totalLength) {
            const progress = Math.round((downloadedLength / totalLength) * 100);
            if (progress % 5 === 0) {
              db.prepare('UPDATE episodes SET progress = ? WHERE id = ?').run(progress, episode.id);
            }
          }
          yield chunk;
        }
      },
      fs.createWriteStream(filePath)
    );

    db.prepare('UPDATE episodes SET download_status = ?, progress = 100 WHERE id = ?')
      .run('completed', episode.id);
    downloadAttempts.delete(episode.id);
  } catch (error) {
    if (fs.existsSync(filePath)) fs.unlinkSync(filePath);
    
    // Check for global outage
    syncState.failedFeeds.add(episode.feed_id);
    const activeTasks = db.prepare('SELECT COUNT(*) as count FROM episodes WHERE download_status IN ("pending", "downloading")').get().count;
    const failureRate = activeTasks > 0 ? (syncState.failedFeeds.size / Math.max(activeTasks, 5)) : 0;

    if (syncState.failedFeeds.size >= 5 || failureRate >= 0.5) {
      console.error(`Failure alert. Initiating global pause for 1 hour.`);
      syncState.globalPauseUntil = Date.now() + 60 * 60 * 1000;
      syncState.failedFeeds.clear();
    }
    
    const nextAttemptIndex = attempts;
    if (nextAttemptIndex < RETRY_SCHEDULE_MINUTES.length) {
      const waitTimeMinutes = RETRY_SCHEDULE_MINUTES[nextAttemptIndex];
      downloadAttempts.set(episode.id, attempts + 1);
      
      console.warn(`Download failed for ${episode.title} (attempt ${nextAttemptIndex + 1}), retrying in ${waitTimeMinutes} minutes...`);
      setTimeout(() => downloadQueue.add(() => downloadEpisode(episode)), waitTimeMinutes * 60 * 1000);
      
      db.prepare('UPDATE episodes SET download_status = ?, error_message = ? WHERE id = ?')
        .run('pending', `Attempt ${nextAttemptIndex + 1} failed: ${error.message}`, episode.id);
    } else {
      downloadAttempts.delete(episode.id);
      db.prepare('UPDATE episodes SET download_status = ?, error_message = ? WHERE id = ?')
        .run('failed', error.message, episode.id);
      console.error(`Download failed permanently: ${episode.title}`, error.message);
    }
  }
}

if (process.setPriority) {
  try { process.setPriority(19); } catch (e) { console.warn('Could not set priority', e); }
}

async function archiveSyncWorker() {
  const feeds = db.prepare('SELECT * FROM feeds').all();
  if (feeds.length === 0) return;

  if (syncState.feedIndex >= feeds.length) syncState.feedIndex = 0;
  const feed = feeds[syncState.feedIndex];
  
  try {
    const feedData = await parser.parseURL(feed.xml_url);
    const allEpisodes = feedData.items;

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
      }
    }
  } catch (err) {
    console.error(`Archival sync failed for ${feed.title}:`, err.message);
  }
  syncState.feedIndex++;
}

setInterval(archiveSyncWorker, 60 * 1000);

const pending = db.prepare("SELECT * FROM episodes WHERE download_status IN ('pending', 'downloading')").all();
for (const ep of pending) {
  downloadQueue.add(() => downloadEpisode(ep));
}

module.exports = { archiveSyncWorker, downloadEpisode };
