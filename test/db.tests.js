const LimitDB  = require('../lib/db');
const MockDate = require('mockdate');
const assert   = require('chai').assert;
const async    = require('async');
const _        = require('lodash');
const ms       = require('ms');

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

describe('LimitDB', () => {
  it('should throw an error when path is not specified', () => {
    assert.throws(() => new LimitDB({}), /path is required/);
  });

  it('should not throw an error when path is not specified and inMemory: true', () => {
    assert.doesNotThrow(() => new LimitDB({ inMemory: true }), /path is required/);
  });

  afterEach(function () {
    MockDate.reset();
  });


  describe('take', () => {
    const db = new LimitDB({
      inMemory: true,
      types
    });

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
        assert.ok(results.every(r => r.conformant));
        db.take({ type: 'ip', key: '8.8.8.8' }, (err, response) => {
          assert.notOk(response.conformant);
          done();
        });
      });
    });
  });

  describe('PUT', function () {
    const db = new LimitDB({
      inMemory: true,
      types
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
    const db = new LimitDB({
      inMemory: true,
      types
    });

    it.skip('should return a list of buckets matching the prefix', (done) => {
      async.map(_.range(10), (i, done) => {
        db.take({ type: 'ip', key: `some-prefix-${i}` }, done);
      }, (err, results) => {
        if (err) return done(err);
        assert.ok(results.every(r => r.conformant));
        db.status({ type: 'ip', prefix: 'some-prefix' }, (err, result) => {
          assert.equal(result.items.length, 10);
          for(var i = 0; i < 10; i++) {
            assert.equal(result.items[i].key, `some-prefix-${i}`);
            assert.euqal(result.items[i].size, 10);
            assert.euqal(result.items[i].reset, 10);
          }
          done();
        });
      });

      // const ip = '211.11.84.12';
      // db.take({ type: 'ip', key: ip}, function (err) {
      //   if (err) return done(err);
      //   db.status({ type: 'ip', prefix: ip }, function (err, response) {
      //     if (err) return done(err);
      //     assert.equal(response.items[0].remaining, 9);
      //     done();
      //   });
      // });
    });

    // it('should work for unlimited', function (done) {
    //   var now = 1425920267;
    //   MockDate.set(now * 1000);
    //   client.take('ip', '0.0.0.0', function (err) {
    //     if (err) return done(err);
    //     client.status('ip', '0.0.0.0', function (err, response) {
    //       if (err) return done(err);
    //       assert.equal(response.items.length, 0);
    //       done();
    //     });
    //   });
    // });

    // it('should work for fixed buckets', function (done) {
    //   client.take('wrong_password', 'curichiaga', function (err) {
    //     if (err) return done(err);
    //     client.status('wrong_password', 'curichiaga', function (err, response) {
    //       if (err) return done(err);
    //       assert.equal(response.items[0].remaining, 2);
    //       done();
    //     });
    //   });
    // });


    // it('should not return fulfilled fixed buckets', function (done) {
    //   client.take('wrong_password', 'catrasca', function (err) {
    //     if (err) return done(err);
    //     client.put('wrong_password', 'catrasca', function (err) {
    //       if (err) return done(err);
    //       client.status('wrong_password', 'catrasca', function (err, response) {
    //         if (err) return done(err);
    //         assert.equal(response.items.length, 0);
    //         done();
    //       });
    //     });
    //   });
    // });

    // it('should not fail if bucket doesnt exists', function (done) {
    //   client.status('ip', '12312312321312321', function (err, response) {
    //     if (err) return done(err);
    //     assert.equal(response.items.length, 0);
    //     done();
    //   });
    // });

    // it.skip('should work with subclasses', function (done) {

    //   async.parallel([
    //     function (cb) { client.take('ip', 'class1|192.123.21.1', cb); },
    //     function (cb) { client.take('ip', 'class1|192.123.21.2', cb); },
    //     function (cb) { client.take('ip', 'class1|192.123.21.2', cb); },
    //     function (cb) { client.take('ip', 'class2|192.123.21.3', cb); },
    //   ], function (err) {
    //     if (err) return done(err);
    //     //this will retrieve all bucket instances of ip - class1
    //     client.status('ip', 'class1', function (err, response) {
    //       if (err) return done(err);
    //       assert.equal(response.items.length, 2);
    //       assert.equal(response.items[0].remaining, 9);
    //       assert.equal(response.items[0].instance, 'class1|192.123.21.1');
    //       assert.equal(response.items[1].remaining, 8);
    //       assert.equal(response.items[1].instance, 'class1|192.123.21.2');
    //       done();
    //     });
    //   });

    // });

  });
});
