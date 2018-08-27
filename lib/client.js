const _ = require('lodash');
const retry = require('retry');
const utils = require('./utils');
const LimitDBRedis = require('./db');
const disyuntor = require('disyuntor');
const EventEmitter = require('events').EventEmitter;

const ValidationError = utils.LimitdRedisValidationError;

const circuitBreakerDefaults = {
  timeout: '0.5s',
  maxFailures: 5,
  cooldown: '1s',
  maxCooldown: '10s',
  name: 'limitd.redis',
  trigger: (err) => {
    return !(err instanceof ValidationError);
  }
};

const retryDefaults = {
  retries: 1,
  minTimeout: 200,
  maxTimeout: 800
};

class LimitdRedis extends EventEmitter {
  constructor(params) {
    super();

    this.db = new LimitDBRedis(_.pick(params, ['uri', 'nodes', 'buckets', 'prefix']));

    this.db.on('error', (err) => {
      this.emit('error', err);
    });

    this.db.on('ready', () => {
      this.emit('ready');
    });

    this.breakerOpts = _.merge(circuitBreakerDefaults, params.circuitbreaker);
    this.retryOpts = _.merge(retryDefaults, params.retry);

    this.dispatch = disyuntor(this.dispatch.bind(this), this.breakerOpts);
  }

  static buildParams(type, key, opts, cb) {
    const params = { type, key };
    const optsType = typeof opts;

    // handle lack of opts and/or cb
    if (cb == null) {
      if (optsType === 'function') {
        cb = opts;
        opts = undefined;
      } else {
        cb = _.noop;
      }
    }

    if (optsType === 'number') {
      params.count = opts;
    }

    if (optsType === 'object') {
      _.assign(params, opts);
    }

    return [params, cb];
  }

  handler(method, type, key, opts, cb) {
    let [params, callback] = LimitdRedis.buildParams(type, key, opts, cb);
    this.dispatch(method, params, callback);
  }

  dispatch(method, params, cb) {
    const operation = retry.operation(this.retryOpts);
    operation.attempt(() => {
      this.db[method](params, (err, results) => {
        if (err instanceof ValidationError) {
          return cb(err);
        }

        if (operation.retry(err)) {
          return;
        }

        return cb(err ? operation.mainError() : null, results);
      });
    });
  }

  take(type, key, opts, cb) {
    this.handler('take', type, key, opts, cb);
  }

  wait(type, key, opts, cb) {
    this.handler('wait', type, key, opts, cb);
  }

  put(type, key, opts, cb) {
    this.handler('put', type, key, opts, cb);
  }

  reset(type, key, opts, cb) {
    this.put(type, key, opts, cb);
  }
}

module.exports = LimitdRedis;
module.exports.ValidationError = ValidationError;
