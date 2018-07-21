const _ = require('lodash');
const retry = require('retry');
const LimitDBRedis = require('./db');
const disyuntor = require('disyuntor');
const EventEmitter = require('events').EventEmitter;

const circuitBreakerDefaults = {
  timeout: '0.5s',
  maxFailures: 5,
  cooldown: '1s',
  maxCooldown: '10s',
  name: 'limitd.redis'
};

const retryDefaults = {
  retries: 1,
  minTimeout: 200,
  maxTimeout: 800
};

class LimitdRedis extends EventEmitter {
  constructor(params) {
    super();

    this.db = new LimitDBRedis(_.pick(params, ['uri', 'nodes', 'buckets']));

    this.db.on('error', (err) => {
      this.emit('error', err);
    });

    this.db.on('ready', () => {
      this.emit('ready');
    });

    this.dispatch = disyuntor(this.dispatch.bind(this),
      _.merge(_.pick(params, ['circuitbreaker']), circuitBreakerDefaults));

    this.db.status = disyuntor(this.db.status.bind(this.db),
      _.merge(_.pick(params, ['circuitbreaker']), circuitBreakerDefaults));

    this.retryOpts = _.merge(_.pick(params, ['retry']), retryDefaults);
  }

  handler(method, type, key, count, cb) {
    const countType = typeof count;
    if (cb == null) {
      if (countType === 'function') {
        cb = count;
        count = undefined;
      } else {
        cb = _.noop;
      }
    }
    this.dispatch(method, type, key, count, cb);
  }

  dispatch(method, type, key, count, cb) {
    const operation = retry.operation(this.retryOpts);
    operation.attempt(() => {
      this.db[method]({ type, key, count }, (err, results) => {
        if (operation.retry(err)) {
          return;
        }
        return cb(err ? operation.mainError(): null, results);
      });
    });
  }

  take(type, key, count, cb) {
    this.handler('take', type, key, count, cb);
  }

  wait(type, key, count, cb) {
    this.handler('wait', type, key, count, cb);
  }

  put(type, key, count, cb) {
    this.handler('put', type, key, count, cb);
  }

  status(type, prefix, cb) {
    this.db.status({ type, prefix }, cb);
  }

  reset(type, key, count, cb) {
    this.put(type, key, count, cb);
  }
}

module.exports = LimitdRedis;
