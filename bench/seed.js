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

const count = 1000000;

db.on('ready', () => {
  const nodes = db.redis.nodes('master');
  async.map(nodes, (node, cb) => {
    //node.flushall(cb);
    cb();
  }, (err) => {
    if (err) process.exit(1);

    for (var i = 0; i < count; i++) {
      db.take({ type: 'ip', key: `spop-${i}-p` }, _.noop);
    }
  });
});
