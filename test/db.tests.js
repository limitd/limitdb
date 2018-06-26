const tmp = require('tmp');
const LimitDB  = require('../');
const MockDate = require('mockdate');
const assert   = require('chai').assert;
const async    = require('async');
const _        = require('lodash');
const ms       = require('ms');
const redis    = require('redis');

const types = {
  ip: {
    size: 10,
    per_second: 5,
    overrides: {
      '127.0.0.1': {
        per_second: 100
      },
      'local-lan': {
        match: /192\.168\./,
        per_second: 50
      },
      '10.0.0.123': {
        until: new Date(Date.now() - ms('24h')), //yesterday
        per_second: 50
      },
      '10.0.0.1': {
        size: 1,
        per_hour: 2
      },
      '0.0.0.0': {
        size: 100,
        unlimited: true
      },
      '8.8.8.8': {
        size: 10
      }
    }
  }
};

const configs = {
  'redisdb': () => {
    return {
      driver: 'redisdb',
      types,
      inMemory: true
    }
  },
  'leveldb': () => {
    return {
      types,
      path: tmp.dirSync().name
    };
  },
  'leveldb (inMemory)': () => {
    return {
      types,
      inMemory: true
    };
  },
  'rocksdb': () => {
    return {
      driver: 'rocksdb',
      types,
      path: tmp.dirSync().name
    };
  }
};

function describeForEachConfig(callback) {
  Object.keys(configs).forEach(name => {
    describe(`LimitDB with ${name}`, () => {
      callback(configs[name]);
    });
  });
}

describeForEachConfig((getConfig) => {
  it('should throw an error when path is not specified', () => {
    assert.throws(() => new LimitDB({}), /path is required/);
  });

  it('should not throw an error when path is not specified and inMemory: true', () => {
    assert.doesNotThrow(() => new LimitDB({ inMemory: true }), /path is required/);
  });

  afterEach(function () {
    MockDate.reset();
  });


  describe('TAKE', () => {
    var db;

    before(function(done) {
      const config = getConfig();
      db = new LimitDB(config);
      db.once('ready', () => {
        if (config.driver === 'redisdb') {
          return redis.createClient().flushall(done);
        }

        return done();
      });
    });

    after(function(done) {
      db.close(done);
    })

    it('should fail when type is not provided', (done) => {
      db.take({}, (err) => {
        assert.match(err.message, /type is required/);
        done();
      });
    });

    it('should fail when type is not defined', (done) => {
      db.take({ type: 'cc' }, (err) => {
        assert.match(err.message, /undefined bucket type cc/);
        done();
      });
    });

    it('should fail when key is not provided', (done) => {
      db.take({ type: 'ip' }, (err) => {
        assert.match(err.message, /key is required/);
        done();
      });
    });


    it('should work :)', (done) => {
      const takeParams = { type: 'ip',  key: '21.17.65.41'};
      db.take(takeParams, (err) => {
        if (err) return done(err);
        db.take(takeParams, (err, result) => {
          if (err) { return done(err); }
          assert.equal(result.conformant, true);
          assert.equal(result.remaining, 8);
          done();
        });
      });
    });


    it('should add a ttl to unused buckets', function (done) {
      const params = { type: 'ip', key: '211.45.66.1'};
      db.take(params, function (err) {
        if (err) return done(err);
        setTimeout(function () {
          db._types[params.type].db.get(params.key, function (err, result) {
            assert.isUndefined(result);
            done();
          });
        }, 3000);
      });
    });

    it('should return TRUE with right remaining and reset after filling up the bucket', (done) => {
      var now = 1425920267;
      db.take({
        type: 'ip',
        key:  '5.5.5.5'
      }, function (err) {
        if (err) return done(err);
        db.put({
          type: 'ip',
          key:  '5.5.5.5',
        }, (err) => {
          if (err) return done(err);
          MockDate.set(now * 1000);
          db.take({
            type: 'ip',
            key:  '5.5.5.5'
          }, function (err, result) {
            if (err) return done(err);

            assert.ok(result.conformant);
            assert.equal(result.remaining, 9);
            assert.equal(result.reset, now + 1);
            assert.equal(result.limit, 10);

            done();
          });
        });
      });
    });

    it('should return TRUE when traffic is conformant', (done) => {
      var now = 1425920267;
      MockDate.set(now * 1000);
      db.take({
        type: 'ip',
        key:  '1.1.1.1'
      }, function (err, result) {
        if (err) return done(err);
        assert.ok(result.conformant);
        assert.equal(result.remaining, 9);
        assert.equal(result.reset, now + 1);
        assert.equal(result.limit, 10);
        done();
      });
    });

    it('should return FALSE when requesting more than the size of the bucket', (done) => {
      var now = 1425920267;
      MockDate.set(now * 1000);
      db.take({
        type:  'ip',
        key:   '2.2.2.2',
        count: 12
      }, function (err, result) {
        if (err) return done(err);
        assert.notOk(result.conformant);
        assert.equal(result.remaining, 10);
        assert.equal(result.reset, now);
        assert.equal(result.limit, 10);
        done();
      });
    });

    it('should return FALSE when traffic is not conformant', (done) => {
      const takeParams = {
        type:  'ip',
        key:   '3.3.3.3'
      };
      const now = 1425920267;
      MockDate.set(now * 1000);
      async.map(_.range(10), (i, done) => {
        db.take(takeParams, done);
      }, (err, responses) => {
        if (err) return done(err);
        assert.ok(responses.every(function (r) { return r.conformant; }));
        db.take(takeParams, (err, response) => {
          assert.notOk(response.conformant);
          assert.equal(response.remaining, 0);
          done();
        });
      });
    });

    it('should return TRUE if an override by name allows more', (done) => {
      const takeParams = {
        type:  'ip',
        key:   '127.0.0.1'
      };
      const now = 1425920267;
      MockDate.set(now * 1000);
      async.each(_.range(10), (i, done) => {
        db.take(takeParams, done);
      }, (err) => {
        if (err) return done(err);
        db.take(takeParams, function (err, result) {
          assert.ok(result.conformant);
          assert.ok(result.remaining, 89);
          done();
        });
      });
    });

    it('should return TRUE if an override by regex allows more', (done) => {
      const takeParams = {
        type:  'ip',
        key:   '192.168.0.1'
      };
      const now = 1425920267;
      MockDate.set(now * 1000);
      async.each(_.range(10), (i, done) => {
        db.take(takeParams, done);
      }, (err) => {
        if (err) return done(err);
        db.take(takeParams, function (err, result) {
          assert.ok(result.conformant);
          assert.ok(result.remaining, 39);
          done();
        });
      });
    });


    it('can expire an override', (done) => {
      const takeParams = {
        type: 'ip',
        key:  '10.0.0.123'
      };
      async.each(_.range(10), (i, cb) => {
        db.take(takeParams, cb);
      }, (err) => {
        if (err) return done(err);
        db.take(takeParams, (err, response) => {
          assert.notOk(response.conformant);
          done();
        });
      });
    });

    it('should use seconds ceiling for next reset', (done) => {
      // it takes ~1790 msec to fill the bucket with this test
      const now = 1425920267;
      MockDate.set(now * 1000);
      const requests = _.range(9).map(() => {
        return cb => db.take({ type: 'ip', key: '211.123.12.36' }, cb);
      });
      async.series(requests, function (err, results) {
        if (err) return done(err);
        var lastResult = results[results.length -1];
        assert.ok(lastResult.conformant);
        assert.equal(lastResult.remaining, 1);
        assert.equal(lastResult.reset, now + 2);
        assert.equal(lastResult.limit, 10);
        done();
      });
    });

    it('should set reset to UNIX timestamp regardless of period', function(done){
      var now = 1425920267;
      MockDate.set(now * 1000);
      db.take({ type: 'ip', key: '10.0.0.1' }, (err, result) => {
        if (err) { return done(err); }
        assert.ok(result.conformant);
        assert.equal(result.remaining, 0);
        assert.equal(result.reset, now + 1800);
        assert.equal(result.limit, 1);
        done();
      });
    });

    it('should work for unlimited', (done) => {
      var now = 1425920267;
      MockDate.set(now * 1000);
      db.take({ type: 'ip', key: '0.0.0.0' }, (err, response) => {
        if (err) return done(err);
        assert.ok(response.conformant);
        assert.equal(response.remaining, 100);
        assert.equal(response.reset, now);
        assert.equal(response.limit, 100);
        done();
      });
    });

    it('should work with a fixed bucket', (done) => {
      async.map(_.range(10), (i, done) => {
        db.take({ type: 'ip', key: '8.8.8.8' }, done);
      }, (err, results) => {
        if (err) return done(err);
        results.forEach((r, i) => {
          assert.equal(r.remaining + i + 1, 10);
        });
        assert.ok(results.every(r => r.conformant));
        db.take({ type: 'ip', key: '8.8.8.8' }, (err, response) => {
          assert.notOk(response.conformant);
          done();
        });
      });
    });

  });

  describe('PUT', function () {
    var db;

    before(function(done) {
      const config = getConfig();
      db = new LimitDB(config);
      db.once('ready', () => {
        if (config.driver === 'redisdb') {
          return redis.createClient().flushall(done);
        }

        return done();
      });
    });

    after(function(done) {
      db.close(done);
    });

    it('should restore the bucket when reseting', (done) => {
      const bucketKey = { type: 'ip',  key: '211.123.12.12'};
      db.take(bucketKey, (err) => {
        if (err) return done(err);
        db.put(bucketKey, (err) => {
          if (err) return done(err);
          db.take(bucketKey, function (err, response) {
            if (err) return done(err);
            assert.equal(response.remaining, 9);
            done();
          });
        });
      });
    });

    it('should restore the bucket when reseting with all', (done) => {
      const takeParams = { type: 'ip',  key: '21.17.65.41', count: 9 };
      db.take(takeParams, (err) => {
        if (err) return done(err);
        db.put({ type: 'ip',  key: '21.17.65.41', count: 1, all: true }, (err) => {
          if (err) return done(err);
          db.take(takeParams, function (err, response) {
            if (err) return done(err);
            assert.equal(response.conformant, true);
            assert.equal(response.remaining, 1);
            done();
          });
        });
      });
    });

    it('should be able to reset without callback', (done) => {
      const bucketKey = { type: 'ip',  key: '211.123.12.12'};
      db.take(bucketKey, (err) => {
        if (err) return done(err);
        db.put(bucketKey);
        setImmediate(() => {
          db.take(bucketKey, function (err, response) {
            if (err) return done(err);
            assert.equal(response.remaining, 9);
            done();
          });
        });
      });
    });

    it('should work for a fixed bucket', (done) => {
      db.take({ type: 'ip', key: '8.8.8.8' }, function (err, result) {
        assert.ok(result.conformant);
        db.put({ type: 'ip', key: '8.8.8.8' }, function (err, result) {
          if (err) return done(err);
          assert.equal(result.remaining, 10);
          // assert.isUndefined(result.reset);
          done();
        });
      });
    });
  });

  describe('STATUS', function () {
    var db;

    before(function(done) {
      db = new LimitDB(getConfig());
      db.once('ready', done);
    });

    after(function(done) {
      db.close(done);
    });

    it('should return a list of buckets matching the prefix', (done) => {
      const now = 1425920267;
      MockDate.set(now * 1000);
      async.map(_.range(10), (i, done) => {
        db.take({ type: 'ip', key: `some-prefix-${i}` }, done);
      }, (err, results) => {
        if (err) return done(err);
        assert.ok(results.every(r => r.conformant));
        db.status({ type: 'ip', prefix: 'some-prefix' }, (err, result) => {
          if (err) { return done(err); }
          assert.equal(result.items.length, 10);
          for(var i = 0; i < 10; i++) {
            assert.equal(result.items[i].key, `some-prefix-${i}`);
            assert.equal(result.items[i].limit, 10);
            assert.equal(result.items[i].remaining, 9);
            assert.equal(result.items[i].reset, now + 1);
          }
          done();
        });
      });
    });

    it('should drip on status', (done) => {
      const now = 1425920267;
      MockDate.set(now * 1000);

      db.take({ type: 'ip', key: `187.213.89.1`, count: 10 }, (err) => {
        if (err) { return done(err); }
        MockDate.set((now + 1) * 1000);
        db.status({ type: 'ip', prefix: '187.213.89.1' }, (err, status) => {
          if (err) { return done(err); }
          assert.equal(status.items[0].remaining, 5);
          done();
        });
      });
    });
  });

  describe('WAIT', function () {
    var db;

    before(function(done) {
      db = new LimitDB(getConfig());
      db.once('ready', done);
    });

    after(function(done) {
      db.close(done);
    });

    it('should work with a simple request', (done) => {
      var now = 1425920267;
      MockDate.set(now * 1000);
      db.wait({ type: 'ip', key: '211.76.23.4' }, (err, response) => {
        if (err) return done(err);
        assert.ok(response.conformant);
        assert.notOk(response.delayed);


        assert.equal(response.remaining, 9);
        assert.equal(response.reset, now + 1);

        done();
      });
    });

    it('should be delayed when traffic is non conformant', function (done) {
      db.take({
        type: 'ip',
        key: '211.76.23.5',
        count: 10
      }, (err) => {
        if (err) return done(err);
        const waitingSince = Date.now();
        db.wait({
          type: 'ip',
          key: '211.76.23.5',
          count: 3
        }, function (err, response) {
          if (err) { return done(err); }
          var waited = Date.now() - waitingSince;
          assert.ok(response.conformant);
          assert.ok(response.delayed);
          assert.closeTo(waited, 600, 20);
          done();
        });
      });
    });
  });

  describe('isOpen', function() {
    it('should return false when initializing', function(done) {
      const db = new LimitDB(getConfig());
      assert.notOk(db.isOpen());
      db.once('ready', () => {
        db.close(done);
      });
    });

    it('should return true when is ready', function(done) {
      const db = new LimitDB(getConfig());
      db.once('ready', () => {
        assert.ok(db.isOpen());
        db.close(done);
      });
    });
  });

  describe('close', function() {
    var db;

    before(function(done) {
      db = new LimitDB(getConfig());
      db.once('ready', done);
    });

    it('should close the underlying levelup and call the callback', (done) => {
      db.close((err) => {
        if (err) { return done(err); }
        assert.isOk(db._db.isClosed());
        done();
      });
    });
  });

  describe('close when is not ready', function() {
    it('should close the underlying db and call the callback once is ready', (done) => {
      var ready;
      var db = new LimitDB(getConfig());
      db.once('ready', () => ready = true);
      db.close((err) => {
        if (err) { return done(err); }
        assert.isOk(ready);
        assert.isOk(db._db.isClosed());
        done();
      });
    });
  });

  describe('close when is closed', function() {
    var db;

    before(function(done) {
      db = new LimitDB(getConfig());
      db.once('ready', () => db.close(done));
    });

    it('should close the underlying db and call the callback once is ready', (done) => {
      db.close((err) => {
        assert.match(err.message, /already closed/);
        done();
      });
    });
  });

  describe('loadTypes when the database is open', () => {
    var db;

    before(function(done) {
      db = new LimitDB(getConfig());
      db.once('ready', () => {
        const newTypes = Object.assign({"cc": { per_second: 100 }}, types);
        db.loadTypes(newTypes);
        done();
      });
    });

    after(function(done) {
      db.close(done);
    });

    it('should not fail', (done) => {
      db.take({ type: 'cc', key: '123' }, done);
    });
  });
});
