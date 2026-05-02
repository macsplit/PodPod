const express = require('express');
const path = require('path');
const pino = require('pino-http')();
const db = require('../db');
const fs = require('fs');

const app = express();
const port = process.env.PORT || 3000;

app.use(pino);
app.use(express.json());
app.use(express.static(path.join(__dirname, '../public')));

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

  if (feedId) { conditions.push('e.feed_id = ?'); params.push(feedId); }
  if (status) { conditions.push('e.download_status = ?'); params.push(status); }
  if (search) { conditions.push('e.id IN (SELECT rowid FROM episodes_fts WHERE episodes_fts MATCH ?)'); params.push(search); }

  if (conditions.length > 0) query += ' WHERE ' + conditions.join(' AND ');
  query += ' ORDER BY e.pub_date DESC LIMIT ? OFFSET ?';
  params.push(limit, offset);

  const episodes = db.prepare(query).all(...params);
  
  let countQuery = 'SELECT COUNT(*) as total FROM episodes e';
  if (conditions.length > 0) countQuery += ' WHERE ' + conditions.join(' AND ');
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
  res.json({ size: 0, pending: 0, downloading });
});

app.get('/play/:id', (req, res) => {
  const episode = db.prepare('SELECT file_path, title FROM episodes WHERE id = ?').get(req.params.id);
  if (!episode || !episode.file_path || !fs.existsSync(episode.file_path)) return res.status(404).send('Not found');

  const stat = fs.statSync(episode.file_path);
  const range = req.headers.range;

  if (range) {
    const parts = range.replace(/bytes=/, "").split("-");
    const start = parseInt(parts[0], 10);
    const end = parts[1] ? parseInt(parts[1], 10) : stat.size - 1;
    res.writeHead(206, {
      'Content-Range': `bytes ${start}-${end}/${stat.size}`,
      'Accept-Ranges': 'bytes',
      'Content-Length': (end - start) + 1,
      'Content-Type': 'audio/mpeg',
    });
    fs.createReadStream(episode.file_path, { start, end }).pipe(res);
  } else {
    res.writeHead(200, { 'Content-Length': stat.size, 'Content-Type': 'audio/mpeg' });
    fs.createReadStream(episode.file_path).pipe(res);
  }
});

app.listen(port, '0.0.0.0', () => console.log(`Server listening on port ${port}`));
