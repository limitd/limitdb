/* eslint-env node, mocha */
const _ = require('lodash');
const assert = require('chai').assert;
const LimitRedis = require('../lib/client');

describe('LimitdRedis', () => {
  let client;
  beforeEach((done) => {
    client = new LimitRedis({ uri: 'localhost', buckets: {} });
    client.on('error', done);
    client.on('ready', done);
  });

  describe('#constructor', () => {
    it('should call error if db fails', (done) => {
      let called = false; // avoid uncaught
      client = new LimitRedis({ uri: 'localhost:fail' });
      client.on('error', () => {
        if (!called) {
          called = true;
          return done();
        }
      });
    });
    it('should set up retry and circuitbreaker', () => {
      assert.equal(client.retryOpts.retries, 1);
      assert.equal(client.retryOpts.minTimeout, 200);
      assert.equal(client.retryOpts.maxTimeout, 800);
    });
  });

  describe('#handler', () => {
    it('should handle count & cb-less calls', (done) => {
      client.db.take = (params, cb) => {
        cb();
        done();
      };
      client.handler('take', 'test', 'test', 1);
    });
    it('should handle count-less & cb-less calls', (done) => {
      client.db.take = (params, cb) => {
        cb();
        done();
      };
      client.handler('take', 'test', 'test');
    });
    it('should handle count-less & cb calls', (done) => {
      client.db.take = (params, cb) => {
        cb();
      };
      client.handler('take', 'test', 'test', done);
    });
    it('should retry', (done) => {
      let calls = 0;
      client.db.take = (params, cb) => {
        if (calls === 0) {
          calls++;
          return cb(new Error());
        }
        return cb();
      };
      client.handler('take', 'test', 'test', done);
    });
    it('should circuitbreak', (done) => {
      client.db.take = () => {};
      client.handler('take', 'test', 'test', _.noop);
      client.handler('take', 'test', 'test', _.noop);
      client.handler('take', 'test', 'test', _.noop);
      client.handler('take', 'test', 'test', _.noop);
      client.handler('take', 'test', 'test', _.noop);
      client.handler('take', 'test', 'test', () => {
        client.handler('take', 'test', 'test', (err) => {
          assert.equal(err.message, 'limitd.redis: the circuit-breaker is open');
          done();
        });
      });
    });
  });

  describe('#take', () => {
    it('should call #handle with take as the method', (done) => {
      client.handler = (method, type, key, count, cb) => {
        assert.equal(method, 'take');
        cb();
      };
      client.take('test', 'test', 1, done);
    });
  });

  describe('#wait', () => {
    it('should call #handle with take as the method', (done) => {
      client.handler = (method, type, key, count, cb) => {
        assert.equal(method, 'wait');
        cb();
      };
      client.wait('test', 'test', 1, done);
    });
  });

  describe('#put', () => {
    it('should call #handle with take as the method', (done) => {
      client.handler = (method, type, key, count, cb) => {
        assert.equal(method, 'put');
        cb();
      };
      client.put('test', 'test', 1, done);
    });
  });

  describe('#reset', () => {
    it('should call #put', (done) => {
      client.put = (type, key, count, cb) => {
        cb();
      };
      client.reset('test', 'test', 1, done);
    });
  });
});
