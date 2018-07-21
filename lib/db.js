const ms = require('ms');
const fs = require('fs');
const _ = require('lodash');
const async = require('async');
const utils = require('./utils');
const Redis = require('ioredis');
const EventEmitter = require('events').EventEmitter;

const PUT_LUA = fs.readFileSync(`${__dirname}/put.lua`, 'utf8');
const TAKE_LUA = fs.readFileSync(`${__dirname}/take.lua`, 'utf8');
const STATUS_LUA = fs.readFileSync(`${__dirname}/status.lua`, 'utf8');

class LimitDBRedis extends EventEmitter {

  /**
   * Creates an instance of LimitDB client for Redis.
   * @param {params} params - The configuration for the database and client.
   */
  constructor(config) {
    super();
    config = config || {};

    if (!config.nodes && !config.uri) {
      throw new Error('Redis connection information must be specified');
    }

    this.redis = null;
    if (config.nodes) {
      this.redis = new Redis.Cluster(config.nodes);
    } else {
      this.redis = new Redis(config.uri);
    }

    this.state = 'init';
    this.buckets = config.buckets ? utils.buildBuckets(config.buckets) : null;

    this.redis.on('ready', () => {
      this.redis.defineCommand('take', {
        numberOfKeys: 1,
        lua: TAKE_LUA
      });

      this.redis.defineCommand('put', {
        numberOfKeys: 1,
        lua: PUT_LUA
      });

      const nodes = this.redis.nodes ? this.redis.nodes('slave') : [this.redis];
      nodes.forEach((node) => {
        node.defineCommand('bucketStatus', {
          numberOfKeys: 0,
          lua: STATUS_LUA
        });
      });

      // TODO: Config Logic

      if (this.buckets) {
        this.state = 'ready';
        this.emit('ready');
      }
    });

    this.redis.on('error', (err) => {
      this.emit('error', err);
    });
  }

  close(callback) {
    this.redis.quit(callback);
  }

  bucketKeyConfig(type, key) {
    const fromCache = type.overridesCache.get(key);

    if (fromCache) {
      return fromCache;
    }

    const result = _.find(type.overrides, (o) => {
      if (o.match) {
        return o.match.exec(key);
      } else {
        return o.name === key;
      }
    }) || type;

    type.overridesCache.set(key, result);

    return result;
  }

  calculateReset(bucketKeyConfig, remaining) {
    if (!bucketKeyConfig.per_interval) {
      return 0;
    }

    const now = Date.now();
    const missing = bucketKeyConfig.size - remaining;
    const msToCompletion = Math.ceil(missing * bucketKeyConfig.interval / bucketKeyConfig.per_interval);
    return Math.ceil((now + msToCompletion) / 1000);
  }

  calculateRemaining(bucketKeyConfig, remaining, lastDrip) {
    if (!bucketKeyConfig.per_interval) {
      return remaining;
    }
    const now = Date.now();
    const deltaMS = Math.max(now - lastDrip, 0);
    const dripAmount = deltaMS * (bucketKeyConfig.per_interval / bucketKeyConfig.interval);
    return Math.min(remaining + dripAmount, bucketKeyConfig.size);
  }

  validateParams(params, attr) {
    if (typeof params !== 'object') {
      return new Error('params are required');
    }

    if (typeof params.type !== 'string') {
      return new Error('type is required');
    }

    if (typeof this.buckets[params.type] === 'undefined') {
      return new Error(`undefined bucket type ${params.type}`);
    }

    if (typeof params[attr] !== 'string') {
      return new Error(`${attr} is required`);
    }
  }

  /**
   * Take N elements from a bucket if available.
   *
   * @param {takeParams} params - The params for take.
   * @param {function(Error, takeResult)} callback.
   */
  take(params, callback) {
    const valError = this.validateParams(params, 'key');
    if (valError) {
      return setImmediate(callback, valError);
    }

    const bucket = this.buckets[params.type];
    const bucketKeyConfig = this.bucketKeyConfig(bucket, params.key);

    const count = params.count || 1;

    if (bucketKeyConfig.unlimited) {
      return setImmediate(callback, null, {
        conformant: true,
        remaining: bucketKeyConfig.size,
        reset: Math.ceil(Date.now() / 1000),
        limit: bucketKeyConfig.size,
        delayed: false
      });
    }

    this.redis.take(`${params.type}:${params.key}`,
      Date.now(),
      bucketKeyConfig.per_interval / bucketKeyConfig.interval || 0,
      bucketKeyConfig.size,
      count,
      bucketKeyConfig.ttl || ms('7d') / 1000,
      (err, results) => {
        if (err) {
          return callback(err);
        }

        return callback(null, {
          conformant: parseInt(results[2], 10) ? true : false,
          remaining: Math.floor(parseInt(results[1], 10)),
          reset: this.calculateReset(bucketKeyConfig, Math.floor(results[1], 10)),
          limit: bucketKeyConfig.size,
          delayed: false
        });
      });
  }

  /**
   * Take N elements from a bucket if available otherwise wait for them.
   * The callback is called when the number of request tokens is available.
   *
   * @param {waitParams} params - The params for take.
   * @param {function(Error, waitResult)} callback.
   */
  wait(params, callback) {
    this.take(params, (err, result) => {
      if (err || result.conformant) {
        return callback(err, result);
      }

      const bucket = this.buckets[params.type];
      const bucketKeyConfig = this.bucketKeyConfig(bucket, params.key);
      const count = params.count || 1;
      const required = count - result.remaining;
      const minWait = Math.ceil(required * bucketKeyConfig.interval / bucketKeyConfig.per_interval);

      return setTimeout(() => {
        this.wait(params, (err, result) => {
          if (err) {
            return callback(err);
          }
          result.delayed = true;
          callback(null, result);
        });
      }, minWait);
    });
  }

  /**
   * Put N elements in the bucket.
   *
   * @param {putParams} params - The params for take.
   * @param {function(Error, putResult)} [callback].
   */
  put(params, callback) {
    callback = callback || _.noop;

    const valError = this.validateParams(params, 'key');
    if (valError) {
      return setImmediate(callback, valError);
    }

    const bucket = this.buckets[params.type];
    const bucketKeyConfig = this.bucketKeyConfig(bucket, params.key);

    const count = params.all ? bucketKeyConfig.size : (params.count || bucketKeyConfig.size);

    if (bucketKeyConfig.unlimited) {
      return setImmediate(callback, null, {
        remaining: bucketKeyConfig.size,
        reset: Math.ceil(Date.now() / 1000),
        limit: bucketKeyConfig.size
      });
    }

    this.redis.put(`${params.type}:${params.key}`,
      Date.now(),
      count,
      bucketKeyConfig.size,
      bucketKeyConfig.ttl || ms('7d') / 1000,
      (err, results) => {
        if (err) {
          return callback(err);
        }

        return callback(null, {
          remaining:  Math.floor(parseInt(results[1], 10)),
          reset: this.calculateReset(bucketKeyConfig, parseInt(results[1], 10)),
          limit: bucketKeyConfig.size
        });
      });
  }


  buildStatusResponse(bucket, prefixLength) {
    return (key, lastDrip, remaining) => {
      const bucketKeyConfig = this.bucketKeyConfig(bucket, key);
      return {
        key: key.substring(prefixLength),
        limit: bucketKeyConfig.size,
        reset: this.calculateReset(bucketKeyConfig, remaining),
        remaining: this.calculateRemaining(bucketKeyConfig, remaining, lastDrip)
      };
    };
  }

  /**
   * Find all buckets that match the key.
   *
   * @param {statusParams} params
   * @param {function(Error, statusResult)} [callback]
   */
  status(params, callback) {
    const valError = this.validateParams(params, 'prefix');
    if (valError) {
      return setImmediate(callback, valError);
    }
    const items = [];
    const limit = params.limit || 0;
    const bucket = this.buckets[params.type];
    const typeLength = `${params.type}:`.length;
    const prefix = `${params.type}:${params.prefix}*`;
    const transform = this.buildStatusResponse(bucket, typeLength);

    const nodes = this.redis.nodes ? this.redis.nodes('slave') : [this.redis];
    async.map(nodes, (node, cb) => {
      node.bucketStatus(prefix, limit, cb);
    }, (err, results) => {
      if (err) {
        return callback(err);
      }

      for (var i = 0; i < results.length; i++) {
        for (var j = 0; j < results[i].length; j++) {
          const match = results[i][j];
          items.push(transform(match[2], parseInt(match[0], 10), parseInt(match[1], 10)));
        }
      }

      return callback(null, { items });
    });
  }
}


module.exports = LimitDBRedis;

/**
 * And now some typedefs for you:
 *
 * @typedef {Object} type
 * @property {integer} [per_interval] The number of tokens to add per interval.
 * @property {integer} [interval] The length of the interval in milliseconds.
 * @property {integer} [size] The maximum number of tokens in the bucket.
 * @property {integer} [per_second] The number of tokens to add per second. Equivalent to "interval: 1000, per_interval: x".
 * @property {integer} [per_minute] The number of tokens to add per minute. Equivalent to "interval: 60000, per_interval: x".
 * @property {integer} [per_hour] The number of tokens to add per hour. Equivalent to "interval: 3600000, per_interval: x".
 * @property {integer} [per_day] The number of tokens to add per day. Equivalent to "interval: 86400000, per_interval: x".
 *
 * @typedef {Object} params
 * @property {string} [params.path] The path to the database.
 * @property {string} [params.driver=leveldb] The driver to use "leveldb" or "rocksdb", defaults to "leveldb".
 * @property {boolean} [params.inMemory] Store the database in RAM instead of disk.
 * @property {Object.<string, type>} params.types The buckets configuration.
 *
 * @typedef takeParams
 * @property {string} type The name of the bucket type.
 * @property {string} key The key of the bucket instance.
 * @property {integer} [count=1] The number of tokens to take from the bucket.
 *
 * @typedef takeResult
 * @property {boolean} conformant Returns true if there is enough capacity in the bucket and the tokens has been removed.
 * @property {integer} remaining The number of tokens remaining in the bucket.
 * @property {integer} reset A unix timestamp indicating when the bucket is going to be full.
 * @property {integer} limit The size of the bucket.
 *
 * @typedef waitParams
 * @property {string} type The name of the bucket type.
 * @property {string} key The key of the bucket instance.
 * @property {integer} [count=1] The number of tokens to wait for.
 *
 * @typedef waitResult
 * @property {integer} remaining The number of tokens remaining in the bucket.
 * @property {integer} reset A unix timestamp indicating when the bucket is going to be full.
 * @property {integer} limit The size of the bucket.
 *
 * @typedef putParams
 * @property {string} type The name of the bucket type.
 * @property {string} key The key of the bucket instance.
 * @property {integer} [count=SIZE] The number of tokens to put in the bucket. Defaults to the size of the bucket.
 *
 * @typedef putResult
 * @property {integer} remaining The number of tokens remaining in the bucket.
 * @property {integer} reset A unix timestamp indicating when the bucket is going to be full.
 * @property {integer} limit The size of the bucket.
 *
 * @typedef statusParams
 * @property {string} type The name of the bucket type.
 * @property {string} prefix The prefix to search for.
 *
 * @typedef statusItem
 * @property {string} key The key of the bucket instance.
 * @property {integer} remaining The number of tokens remaining in the bucket.
 * @property {integer} reset A unix timestamp indicating when the bucket is going to be full.
 * @property {integer} limit The size of the bucket.
 *
 * @typedef statusResult
 * @property {Array.<statusItem>} items The number of tokens remaining in the bucket.
 *
 */
