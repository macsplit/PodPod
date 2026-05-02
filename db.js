const Database = require('better-sqlite3');
const path = require('path');

const dbPath = process.env.DB_PATH || path.join(__dirname, 'podcasts.db');
const db = new Database(dbPath);

// Enable WAL mode for multi-process safety
db.pragma('journal_mode = WAL');
db.pragma('synchronous = NORMAL');

// Initialize schema
db.exec(`
  CREATE TABLE IF NOT EXISTS feeds (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT,
    xml_url TEXT UNIQUE,
    last_checked INTEGER
  );

  CREATE TABLE IF NOT EXISTS episodes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    feed_id INTEGER,
    guid TEXT UNIQUE,
    title TEXT,
    link TEXT,
    pub_date INTEGER,
    description TEXT,
    enclosure_url TEXT,
    file_path TEXT,
    download_status TEXT DEFAULT 'pending',
    progress INTEGER DEFAULT 0,
    error_message TEXT,
    FOREIGN KEY (feed_id) REFERENCES feeds(id)
  );

  CREATE INDEX IF NOT EXISTS idx_episodes_feed_id ON episodes(feed_id);
  CREATE INDEX IF NOT EXISTS idx_episodes_download_status ON episodes(download_status);
  CREATE INDEX IF NOT EXISTS idx_episodes_pub_date ON episodes(pub_date);

  -- Full Text Search
  CREATE VIRTUAL TABLE IF NOT EXISTS episodes_fts USING fts5(title, description, content='episodes', content_rowid='id');

  -- Triggers to keep FTS in sync
  CREATE TRIGGER IF NOT EXISTS episodes_ai AFTER INSERT ON episodes BEGIN
    INSERT INTO episodes_fts(rowid, title, description) VALUES (new.id, new.title, new.description);
  END;
  CREATE TRIGGER IF NOT EXISTS episodes_ad AFTER DELETE ON episodes BEGIN
    INSERT INTO episodes_fts(episodes_fts, rowid, title, description) VALUES('delete', old.id, old.title, old.description);
  END;
  CREATE TRIGGER IF NOT EXISTS episodes_au AFTER UPDATE ON episodes BEGIN
    INSERT INTO episodes_fts(episodes_fts, rowid, title, description) VALUES('delete', old.id, old.title, old.description);
    INSERT INTO episodes_fts(rowid, title, description) VALUES (new.id, new.title, new.description);
  END;
`);

module.exports = db;
