#!/usr/bin/env node

const Parser = require('rss-parser');
const db = require('./db');

const parser = new Parser();
const feedUrl = process.argv[2];

if (!feedUrl) {
  console.error('Usage: ./add-feed.js <url>');
  process.exit(1);
}

async function addFeed() {
  try {
    console.log(`Checking feed: ${feedUrl}...`);
    const feed = await parser.parseURL(feedUrl);
    
    if (!feed.items || feed.items.length === 0) {
      console.error('Error: Feed exists but contains no items.');
      process.exit(1);
    }

    console.log(`Feed found: "${feed.title}" (${feed.items.length} items)`);
    
    const stmt = db.prepare('INSERT INTO feeds (title, xml_url) VALUES (?, ?)');
    stmt.run(feed.title, feedUrl);
    
    console.log('Successfully added feed to database.');
  } catch (err) {
    console.error('Error adding feed:', err.message);
    process.exit(1);
  }
}

addFeed();
