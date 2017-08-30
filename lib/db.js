'use strict';

const level  = require('levelup');
const ttl    = require('level-ttl');
const spaces = require('level-spaces');
const ms     = require('ms');
const _      = require('lodash');
const gms    = require('./gms');
const LRU    = require('lru-cache');

const INTERVAL_TO_MS = {
  'per_second': ms('1s'),
  'per_minute': ms('1m'),
  'per_hour':   ms('1h'),
  'per_day':    ms('1d')
};

const INTERVAL_SHORTCUTS = Object.keys(INTERVAL_TO_MS);
const GC_GRACE_PERIOD = ms('2m');


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

  type.ttl = (type.size * type.interval) / type.per_interval;

  if (process.env.NODE_ENV !== 'test') {
    type.ttl += GC_GRACE_PERIOD;
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

class LimitDB {
  constructor(params) {
    params = params || {};

    if (typeof params.path !== 'string' && !params.inMemory) {
      throw new Error('path is required');
    }
    const path = !params.inMemory ? params.path : undefined;

    this._db = level(path, {
      db: params.inMemory ? require('memdown') : undefined,
      valueEncoding: 'json'
    });

    if (!params.inMemory) {
      this._db = ttl(this._db, {
        checkFrequency: process.env.NODE_ENV === 'test' ? 100 : ms('30s')
      });
    } else {
      if (process.env.NODE_ENV !== 'test') {
        console.warn('inMemory doesnt support GC yet');
      }
    }

    this._types = _.reduce(params.types, (result, typeParams, name) => {
      const type = result[name] = normalizeType(typeParams);
      type.db = gms(spaces(this._db, name, { valueEncoding: 'json' }));
      return result;
    }, {});
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

      bucket = bucket ? this._drip(bucket, typeParams) : {
        lastDrip: Date.now(),
        content: typeParams.size
      };

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
      return bucket;
    }, { ttl: typeParams.ttl }, (err, bucket) => {
      if (err) {
        return callback(err);
      }
      callback(null, {
        remaining:  Math.floor(bucket.content),
        reset:      bucket.reset,
        limit:      typeParams.size
      });
    });
  }

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

      var bucket = JSON.parse(data.value);

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
}


module.exports = LimitDB;
