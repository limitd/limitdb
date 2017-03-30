const bench = require('bench');
const async = require('async');
const _ = require('lodash');
const rimraf = require('rimraf');
const path = require('path');
const fs = require('fs');
const randomstring = require('randomstring');

const LimitDB = require('../lib/db');

const types = {
  ip: {
    size: 10,
    per_second: 5
  }
};

const dbBase = path.join(__dirname, 'db');

if (fs.existsSync(dbBase)) {
  rimraf.sync(dbBase);
}

fs.mkdirSync(dbBase);

const db1 = new LimitDB({
  path: path.join(dbBase, 'db1'),
  types
});


const db2 = new LimitDB({
  path: path.join(dbBase, 'db2'),
  types,
  flushInterval: '5m'
});


exports.compareCount = 10;
exports.countPerLap = 10000;

const fixture = _.range(exports.compareCount).map(() => {
  return _.range(exports.countPerLap).map(() => {
    return randomstring.generate(4);
  });
});

var lapOnTest1 = 0, lapOnTest2 = 0;

exports.compare = {
  'random take on disk db' : function (done) {
    const keys = fixture[lapOnTest1++];
    async.forEach(keys, (key, done) => {
      db1.take({
        type: 'ip',
        key
      }, (err) => {
        if (err) { console.error(err.message); process.exit(1); }
        done();
      });
    }, done);
  },
  'random take with flushing' : function (done) {
    const keys = fixture[lapOnTest2++];
    async.forEach(keys, (key, done) => {
      db2.take({
        type: 'ip',
        key
      }, (err) => {
        if (err) { console.error(err.message); process.exit(1); }
        done();
      });
    }, done);
  }
};

exports.done = (results) => {
  bench.show(results);
  process.exit(0);
};

require("bench").runMain();
