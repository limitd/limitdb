const _ = require('lodash');
const async = require('async');
const DB = require('../lib/db');

const buckets = {
  ip: {
    size: 10,
    per_second: 5
  }
};

const db = new DB({ nodes: [{ port: 7000 }], buckets });

const count = 100000;

db.on('ready', () => {
  const nodes = db.redis.nodes('master');
  async.map(nodes, (node, cb) => {
    node.flushall(cb);
  }, (err) => {
    if (err) process.exit(1);
    async.map(_.range(count), (i, cb) => {
      db.take({ type: 'ip', key: i % 2 ? `prefix-${i}`: `test-${i}` }, cb);
    }, (err) => {
      if (err) process.exit(1);
      console.time('status');

      db.status({ type: 'ip', prefix: 'prefix-'}, (err, res) => {
        if (err) process.exit(1);
        console.timeEnd('status');
        console.log('total:', res.items.length);
        process.exit(0);
      });
    });
  });
});
