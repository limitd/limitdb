const DB = require('../lib/db');

const buckets = {
  ip: {
    size: 10,
    per_second: 5
  }
};

const db = new DB({ nodes: [{ port: 7000 }], buckets });

db.on('ready', () => {
  console.time('status');
  db.status({ type: 'ip', prefix: 'some-prefix-'}, (err, res) => {
    if (err) process.exit(1);
    console.timeEnd('status');
    console.log('total:', res.items.length);
    process.exit(0);
  });
});
