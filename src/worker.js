const fs = require('fs');
const path = require('path');
const { pipeline } = require('stream/promises');
const Parser = require('rss-parser');
const axios = require('axios');
const db = require('../db');

const parser = new Parser();
const STORAGE_ROOT = process.env.STORAGE_ROOT || path.join(__dirname, '../data');

// Track sync state
let syncState = {
  feedIndex: 0
};

// Track attempts per episode
let downloadAttempts = new Map();

async function downloadEpisode(episode) {
  const attempts = downloadAttempts.get(episode.id) || 0;
  
  const feedDir = path.join(STORAGE_ROOT, (episode.feed_title || 'unknown').replace(/[<>:"/\\|?*]/g, '_'));
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
    
    const newAttempts = attempts + 1;
    if (newAttempts < 3) {
      downloadAttempts.set(episode.id, newAttempts);
      console.warn(`Download failed for ${episode.title} (attempt ${newAttempts}), retrying in ${newAttempts * 5} minutes...`);
      setTimeout(() => downloadEpisode(episode), newAttempts * 5 * 60 * 1000);
      db.prepare('UPDATE episodes SET download_status = ?, error_message = ? WHERE id = ?')
        .run('pending', `Attempt ${newAttempts} failed: ${error.message}`, episode.id);
    } else {
      downloadAttempts.delete(episode.id);
      db.prepare('UPDATE episodes SET download_status = ?, error_message = ? WHERE id = ?')
        .run('failed', error.message, episode.id);
      console.error(`Download failed permanently: ${episode.title}`, error.message);
    }
  }
}

// Set low priority for background worker
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
    console.log(`Syncing: ${feed.title} completed.`);
  } catch (err) {
    console.error(`Archival sync failed for ${feed.title}:`, err.message);
  }
  
  syncState.feedIndex++;
}

// Background scheduler - run sync every minute
setInterval(archiveSyncWorker, 60 * 1000);

// Initial queue resume
const pending = db.prepare("SELECT * FROM episodes WHERE download_status IN ('pending', 'downloading')").all();
for (const ep of pending) {
  downloadEpisode(ep).catch(console.error);
}

module.exports = {
  archiveSyncWorker,
  downloadEpisode
};
