const fs = require('fs');
const path = require('path');
const { pipeline } = require('stream/promises');
const Parser = require('rss-parser');
const axios = require('axios');
const { default: PQueue } = require('p-queue'); // Re-introduce p-queue
const db = require('../db');

const parser = new Parser();
const downloadQueue = new PQueue({ concurrency: 5 }); // Limit concurrency
const STORAGE_ROOT = process.env.STORAGE_ROOT || path.join(__dirname, '../data');

// Track sync state
let syncState = {
  feedIndex: 0,
  globalPauseUntil: 0,
  failedFeeds: new Set()
};

// ... inside downloadEpisode catch block:

  } catch (error) {
    if (fs.existsSync(filePath)) fs.unlinkSync(filePath);
    
    // Check for global outage (track unique failed feeds)
    syncState.failedFeeds.add(episode.feed_id);
    
    // Get count of pending/downloading to check percentage failure
    const activeTasks = db.prepare('SELECT COUNT(*) as count FROM episodes WHERE download_status IN ("pending", "downloading")').get().count;
    const failureRate = activeTasks > 0 ? (syncState.failedFeeds.size / Math.max(activeTasks, 5)) : 0;

    if (syncState.failedFeeds.size >= 5 || failureRate >= 0.5) {
      console.error(`Failure alert (Feeds: ${syncState.failedFeeds.size}, Active: ${activeTasks}). Initiating global pause for 1 hour.`);
      syncState.globalPauseUntil = Date.now() + 60 * 60 * 1000;
      syncState.failedFeeds.clear();
    }
    
    const nextAttemptIndex = attempts; // attempts 0, 1, 2, 3
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
  downloadQueue.add(() => downloadEpisode(ep));
}

module.exports = {
  archiveSyncWorker,
  downloadEpisode
};
