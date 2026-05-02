const Parser = require('rss-parser');
const parser = new Parser();
async function test() {
  try {
    const feed = await parser.parseURL('https://feeds.simplecast.com/54nAGcIl');
    console.log('Title:', feed.title);
    console.log('Items count:', feed.items.length);
  } catch (e) {
    console.error(e);
  }
}
test();
