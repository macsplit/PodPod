const express = require('express');
const path = require('path');
const cron = require('node-cron');
const pino = require('pino-http')();
const db = require('./db');
const { checkFeeds, archiveSyncWorker, resumeDownloads, downloadQueue, initFeedsFromOpml } = require('./downloader');
const fs = require('fs');

const app = express();
const port = process.env.PORT || 3000;

app.use(pino);
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// API Endpoints
app.get('/api/feeds', (req, res) => {
  const feeds = db.prepare('SELECT * FROM feeds ORDER BY title ASC').all();
  res.json(feeds);
});

app.get('/api/episodes', (req, res) => {
  const { page = 1, limit = 20, feedId, status, search } = req.query;
  const offset = (page - 1) * limit;

  let query = `
    SELECT e.*, f.title as feed_title 
    FROM episodes e 
    JOIN feeds f ON e.feed_id = f.id
  `;
  const params = [];

  const conditions = [];
  if (feedId) {
    conditions.push('e.feed_id = ?');
    params.push(feedId);
  }
  if (status) {
    conditions.push('e.download_status = ?');
    params.push(status);
  }
  if (search) {
    conditions.push('e.id IN (SELECT rowid FROM episodes_fts WHERE episodes_fts MATCH ?)');
    params.push(search);
  }

  if (conditions.length > 0) {
    query += ' WHERE ' + conditions.join(' AND ');
  }

  query += ' ORDER BY e.pub_date DESC LIMIT ? OFFSET ?';
  params.push(limit, offset);

  const episodes = db.prepare(query).all(...params);
  
  // Get total count for pagination
  let countQuery = 'SELECT COUNT(*) as total FROM episodes e';
  if (conditions.length > 0) {
    countQuery += ' WHERE ' + conditions.join(' AND ');
  }
  const { total } = db.prepare(countQuery).get(...params.slice(0, -2));

  res.json({ episodes, total, page: parseInt(page), limit: parseInt(limit) });
});

app.get('/api/queue', (req, res) => {
  const downloading = db.prepare(`
    SELECT e.*, f.title as feed_title 
    FROM episodes e 
    JOIN feeds f ON e.feed_id = f.id 
    WHERE e.download_status = 'downloading'
  `).all();
  
  res.json({
    size: downloadQueue.size,
    pending: downloadQueue.pending,
    downloading
  });
});

app.post('/api/check', async (req, res) => {
  checkFeeds(); // Run in background
  res.json({ message: 'Check started' });
});

app.get('/play/:id', (req, res) => {
  const episode = db.prepare('SELECT file_path, title FROM episodes WHERE id = ?').get(req.params.id);
  if (!episode || !episode.file_path || !fs.existsSync(episode.file_path)) {
    return res.status(404).send('Episode not found or not downloaded');
  }

  const stat = fs.statSync(episode.file_path);
  const fileSize = stat.size;
  const range = req.headers.range;

  if (range) {
    const parts = range.replace(/bytes=/, "").split("-");
    const start = parseInt(parts[0], 10);
    const end = parts[1] ? parseInt(parts[1], 10) : fileSize - 1;
    const chunksize = (end - start) + 1;
    const file = fs.createReadStream(episode.file_path, { start, end });
    const head = {
      'Content-Range': `bytes ${start}-${end}/${fileSize}`,
      'Accept-Ranges': 'bytes',
      'Content-Length': chunksize,
      'Content-Type': 'audio/mpeg',
    };
    res.writeHead(206, head);
    file.pipe(res);
  } else {
    const head = {
      'Content-Length': fileSize,
      'Content-Type': 'audio/mpeg',
    };
    res.writeHead(200, head);
    fs.createReadStream(episode.file_path).pipe(res);
  }
});

// Initialization
async function start() {
  const opmlPath = path.join(__dirname, 'feed.opml');
  if (fs.existsSync(opmlPath)) {
    console.log('Initializing feeds from OPML...');
    await initFeedsFromOpml(opmlPath);
  }

  resumeDownloads();
  checkFeeds();

  // Schedule periodic checks (every 4 hours)
  cron.schedule('0 */4 * * *', () => {
    console.log('Running scheduled feed check...');
    checkFeeds();
  });

  // Schedule archival sync worker (every 10 minutes)
  cron.schedule('*/10 * * * *', () => {
    archiveSyncWorker();
  });

  app.listen(port, '0.0.0.0', () => {
    console.log(`Server listening on port ${port}`);
  });
}

start().catch(err => {
  console.error('Failed to start server:', err);
  process.exit(1);
});
