'use strict';

const level  = require('levelup');
const ttl    = require('level-ttl');
const spaces = require('level-spaces');
const ms     = require('ms');
const fs     = require('fs');
const _      = require('lodash');
const leveldb = require('leveldown');
const rocksdb = require('rocksdb');

const gms    = require('./gms');
const LRU    = require('lru-cache');
const EventEmitter = require('events').EventEmitter;
const dbChecker = require('../bin/db-checker');

const INTERVAL_TO_MS = {
  'per_second': ms('1s'),
  'per_minute': ms('1m'),
  'per_hour':   ms('1h'),
  'per_day':    ms('1d')
};

const drivers = {
  'rocksdb': rocksdb,
  'leveldb': leveldb
};

const INTERVAL_SHORTCUTS = Object.keys(INTERVAL_TO_MS);
const GC_GRACE_PERIOD = ms('2m');

const defaults = {
  driver: 'leveldb',
  inMemory: false
};

function normalizeType(params) {
  const type = _.pick(params, [
    'per_interval',
    'interval',
    'size',
    'unlimited',
    'depends_on'
  ]);

  INTERVAL_SHORTCUTS.forEach(ish => {
    if (!params[ish]) { return; }
    type.interval = INTERVAL_TO_MS[ish];
    type.per_interval = params[ish];
  });

  if (typeof type.size === 'undefined') {
    type.size = type.per_interval;
  }

  if (type.per_interval) {
    type.ttl = (type.size * type.interval) / type.per_interval;

    if (process.env.NODE_ENV !== 'test') {
      type.ttl += GC_GRACE_PERIOD;
    }
  }

  type.overrides = _.map(params.overrides || params.override || {}, (overrideDef, name) => {
    const override = normalizeType(overrideDef);
    override.name = name;
    override.until = overrideDef.until;
    override.match = overrideDef.match;
    return override;
  }).filter(o => !o.until || o.until >= new Date());

  if (type.overrides) {
    type.overridesCache = new LRU({ max: 50 });
  }

  return type;
}

class LimitDB extends EventEmitter {

  /**
   * Creates an instance of LimitDB.
   * @param {params} params - The configuration for the database.
   */
  constructor(params) {
    super();
    params = params || {};

    params = Object.assign({}, defaults, params);

    if (typeof params.path !== 'string' && !params.inMemory) {
      throw new Error('path is required');
    }

    this.state = 'opening';

    const done = (err) => {
      if (err) {
        return this.emit('error', err);
      }
      this.state = 'ready';
      this.emit('ready');
    };

    const leveldbOnFs = params.driver === 'leveldb' && !params.inMemory;

    if (leveldbOnFs && fs.existsSync(params.path)) {
      this._safeOpenDb(params, done);
    } else {
      this._openDb(params, done);
    }
  }

  _safeOpenDb(params, done) {
    dbChecker.check(params.path, (err) => {
      if (err) {
        this.emit('repairing');
        return leveldb.repair(params.path, (err) => {
          if (err) {
            this.emit('error', err);
            return;
          }
          this._openDb(params, done);
        });
      }
      this._openDb(params, done);
    });
  }

  _openDb(params, callback) {
    const db = drivers[params.driver];
    const memory = params.inMemory && params.driver === 'leveldb' || undefined;

    level(db(params.path || ''), {
      memory,
      valueEncoding: 'json'
    }, (err, db) => {
      if (err) {
        return callback(err);
      }

      this._db = ttl(db, {
        checkFrequency: process.env.NODE_ENV === 'test' ? 100 : ms('30s')
      });

      this.loadTypes(params.types);

      callback();
    });
  }

  /**
   * Load the buckets configuration.
   * This method can be used when the database is open.
   *
   * @param {Object.<string, type>} typesConfig The buckets configuration.
   * @memberof LimitDB
   */
  loadTypes(typesConfig) {
    if (!this._db) {
      throw new Error('database is not ready yet');
    }

    const types = _.reduce(typesConfig, (result, typeParams, name) => {
      const type = result[name] = normalizeType(typeParams);
      type.db = gms(spaces(this._db, name, { valueEncoding: 'json' }));
      return result;
    }, {});

    this._types = types;
  }

  _drip(bucket, type) {
    if (!type.per_interval) {
      return {
        content: bucket.content,
        size: bucket.size
      };
    }

    const now = Date.now();
    const deltaMS = Math.max(now - bucket.lastDrip, 0);
    const dripAmount = deltaMS * (type.per_interval / type.interval);
    const content = Math.min(bucket.content + dripAmount, type.size);

    return {
      content:    content,
      lastDrip:   now,
      size:       type.size,
      beforeDrip: bucket.content
    };
  }

  _getResetTimestamp(bucket, type) {
    if (!type.per_interval) {
      return 0;
    }

    const now = Date.now();
    const missing = type.size - bucket.content;
    const msToCompletion = Math.ceil(missing * type.interval / type.per_interval);

    return Math.ceil((now + msToCompletion) / 1000);
  }

  _getTypeParams(type, key) {
    const fromCache = type.overridesCache.get(key);

    if (fromCache) {
      return fromCache;
    }

    const result = _.find(type.overrides, o => {
      if (o.match) {
        return o.match.exec(key);
      } else {
        return o.name === key;
      }
    }) || type;

    type.overridesCache.set(key, result);

    return result;
  }

  _getFullBucket(typeParams) {
    return {
      lastDrip: Date.now(),
      content: typeParams.size,
      reset: this._getResetTimestamp({ content: typeParams.size }, typeParams.size)
    };
  }



  /**
   * Take N elements from a bucket if available.
   *
   * @param {takeParams} params - The params for take.
   * @param {function(Error, takeResult)} callback - The callback to call
   */
  take(params, callback) {
    if (typeof params !== 'object') {
      params = {};
    }

    if (typeof params.type !== 'string') {
      return setImmediate(callback, new Error('type is required'));
    }

    const type = this._types[params.type];

    if (typeof type === 'undefined') {
      return setImmediate(callback, new Error(`undefined bucket type ${params.type}`));
    }

    if (typeof params.key !== 'string') {
      return setImmediate(callback, new Error('key is required'));
    }

    const typeParams = this._getTypeParams(type, params.key);

    const count = params.count || 1;

    if (typeParams.unlimited) {
      return setImmediate(callback, null, {
        conformant: true,
        remaining: typeParams.size,
        reset: Math.ceil(Date.now() / 1000),
        limit: typeParams.size
      });
    }

    type.db.gms(params.key, (bucket) => {
      if (bucket && typeof bucket === 'string') {
        bucket = JSON.parse(bucket);
      }

      bucket = bucket ? this._drip(bucket, typeParams) : this._getFullBucket(typeParams);

      //this happen when we scale down a bucket
      //imagine the size was 10 and the current content is 9.
      //then we scale down the bucket to 6...
      //The current content should be computed as 6, not 9.
      bucket.content = Math.min(typeParams.size, bucket.content);
      bucket.size = typeParams.size;

      if (bucket.content >= count) {
        bucket.lastConformant = true;
        bucket.content -= count;
      } else {
        bucket.lastConformant = false;
      }

      bucket.reset = this._getResetTimestamp(bucket, typeParams);
      return bucket;
    }, { ttl: typeParams.ttl }, (err, bucket) => {
      if (err) { return callback(err); }
      if (bucket.lastConformant && typeParams.depends_on) {
        return this.take(_.extend({}, params, { type: typeParams.depends_on }), (err, dep) => {
          if (err) { return callback(err); }
          callback(null, {
            conformant: dep.conformant,
            remaining: Math.min(bucket.remaining, dep.remaining),
            reset: Math.min(bucket.reset, dep.reset),
            limit: Math.min(bucket.limit, dep.limit),
          });
        });
      }
      callback(null, {
        conformant: bucket.lastConformant,
        remaining:  Math.floor(bucket.content),
        reset:      bucket.reset,
        limit:      typeParams.size
      });
    });
  }

  /**
   * Take N elements from a bucket if available otherwise wait for them.
   * The callback is called when the number of request tokens is available.
   *
   * @param {waitParams} params - The params for take.
   * @param {function(Error, waitResult)} callback
   */
  wait(params, callback) {
    if (typeof params !== 'object') {
      params = {};
    }

    if (typeof params.type !== 'string') {
      return setImmediate(callback, new Error('type is required'));
    }

    const type = this._types[params.type];

    if (typeof type === 'undefined') {
      return setImmediate(callback, new Error(`undefined bucket type ${params.type}`));
    }

    if (typeof params.key !== 'string') {
      return setImmediate(callback, new Error('key is required'));
    }

    const typeParams = this._getTypeParams(type, params.key);

    const count = params.count || 1;

    this.take(params, (err, result) => {
      if (err) { return callback(err); }
      if (result.conformant) { return callback(null, result); }
      const required = count - result.remaining;
      const minWait = Math.ceil(required * typeParams.interval / typeParams.per_interval);
      return setTimeout(() => {
        this.wait(params, (err, result) => {
          if (err) { return callback(err); }
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
   * @param {function(Error, putResult)} [callback]
   */
  put(params, callback) {
    callback = callback || _.noop;
    if (typeof params !== 'object') {
      params = {};
    }

    if (typeof params.type !== 'string') {
      return setImmediate(callback, new Error('type is required'));
    }

    const type = this._types[params.type];

    if (typeof type === 'undefined') {
      return setImmediate(callback, new Error(`undefined bucket type ${params.type}`));
    }

    if (typeof params.key !== 'string') {
      return setImmediate(callback, new Error('key is required'));
    }

    const typeParams = this._getTypeParams(type, params.key);

    if (typeParams.unlimited) {
      return setImmediate(callback, null, {
        remaining: typeParams.size,
        reset: Math.ceil(Date.now() / 1000),
        limit: typeParams.size
      });
    }

    const count = params.all ?
                    typeParams.size :
                    params.count || typeParams.size;

    type.db.gms(params.key, bucket => {
      if (bucket && typeof bucket === 'string') {
        bucket = JSON.parse(bucket);
      }

      bucket = bucket || {
        lastDrip: Date.now(),
        content: typeParams.size
      };

      bucket.content = Math.min(typeParams.size, bucket.content + count);

      // Storage optimization: if the bucket is full we can remove it from DB.
      // this partially fixes an issue causing us to collect items in DB indefinitelly
      // those items in general exceed the existing limit of 100 items
      // we return when calling bucket status
      return bucket.content === bucket.size ? null : bucket;
    }, { ttl: typeParams.ttl }, (err, bucket) => {
      if (err) {
        return callback(err);
      }

      bucket = bucket || this._getFullBucket(typeParams);

      callback(null, {
        remaining:  Math.floor(bucket.content),
        reset:      bucket.reset,
        limit:      typeParams.size
      });
    });
  }


  /**
   * Put N elements in the bucket.
   *
   * @param {statusParams} params - The params for take.
   * @param {function(Error, statusResult)} [callback]
   */
  status(params, callback) {
    if (typeof params !== 'object') {
      return setImmediate(callback, new Error('params is required'));
    }

    if (typeof params.type !== 'string') {
      return setImmediate(callback, new Error('type is required'));
    }

    if (typeof params.prefix !== 'string') {
      return setImmediate(callback, new Error('prefix is required'));
    }

    const type = this._types[params.type];

    if (typeof type === 'undefined') {
      return setImmediate(callback, new Error(`undefined bucket type ${params.type}`));
    }

    const items = [];
    var count = 0;

    const readStream = type.db.createReadStream({
      gte: params.prefix,
      lte: `${params.prefix}~`,
    });

    const finish = () => {
      callback(null, { items });
    };

    readStream.on('data', data => {
      if (!data.value) return;
      var bucket;

      //this is because of an old bug with levelup
      bucket = typeof data.value === 'string' ?
                JSON.parse(data.value) :
                data.value;

      if (bucket.reset === 0 && bucket.content === bucket.size) {
        //we dont care about this.
        return;
      }

      const typeParams = this._getTypeParams(type, data.key);

      count++;
      bucket = this._drip(bucket, typeParams);

      items.push({
        remaining: bucket.content,
        reset:     this._getResetTimestamp(bucket, typeParams),
        limit:     typeParams.size,
        key:       data.key
      });

      if (count === 100) {
        readStream.destroy();
        finish();
      }
    }).once('end', finish).once('error', (err) => {
      if (err.message === 'Unexpected token ~') {
        readStream.destroy();
        return finish();
      }
      callback(null, err);
    });
  }

  /**
   * Close the database.
   *
   * @param {function(Error)} [callback]
   */
  close(callback) {
    callback = callback || _.noop;
    if (this.state === 'opening') {
      return this.once('ready', () => this.close(callback));
    } else if(this.state === 'closed') {
      return setImmediate(callback, new Error('the database is already closed'));
    }

    this.state = 'closed';

    this._db.close(() => {
      this.emit('closed');
      callback();
    });
  }

  /**
   * Check if the database is open.
   *
   * @returns {boolean}
   */
  isOpen() {
    return this._db && !this._db.isClosed();
  }
}


module.exports = LimitDB;

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
