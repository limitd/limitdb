'use strict';

const level  = require('levelup');
const ttl    = require('level-ttl');
// const serial = require('level-serial');
const spaces = require('level-spaces');
const ms     = require('ms');
const _      = require('lodash');
const gms    = require('./gms');

const INTERVAL_TO_MS = {
  'per_second': ms('1s'),
  'per_minute': ms('1m'),
  'per_hour':   ms('1h'),
  'per_day':    ms('1d')
};

const INTERVAL_SHORT_HANDS = Object.keys(INTERVAL_TO_MS);
const GC_GRACE_PERIOD = ms('2m');


function normalizeType(params) {
  const type = _.pick(params, [
    'per_interval',
    'interval',
    'size',
    'unlimited'
  ]);

  INTERVAL_SHORT_HANDS.forEach(ish => {
    if (!params[ish]) { return; }
    type.interval = INTERVAL_TO_MS[ish];
    type.per_interval = params[ish];
  });

  if (typeof type.size === 'undefined') {
    type.size = type.per_interval;
  }

  type.ttl = (type.size * type.interval) / type.per_interval;

  if (process.env.NODE_ENV === 'test') {
    type.ttl += GC_GRACE_PERIOD;
  }

  type.overrides = _.map(params.overrides || {}, (overrideDef, name) => {
    const override = normalizeType(overrideDef);
    override.name = name;
    override.until = overrideDef.until;
    override.match = overrideDef.match;
    return override;
  }).filter(o => !o.until || o.until >= new Date());

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

    this._db = ttl(this._db, {
      checkFrequency: process.env.NODE_ENV === 'test' ? 100 : ms('30s')
    });

    this._types = _.reduce(params.types, (result, typeParams, name) => {
      const type = result[name] = normalizeType(typeParams);
      type.db = gms(spaces(this._db, name, { valueEncoding: 'json' }));
      return result;
    }, {});

  }

  _drip(bucket, type) {
    if (!type.per_interval) {
      return bucket;
    }

    const now = +new Date();
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

    const typeParams = type.overrides.filter(o => {
      if (o.match) {
        return o.match.exec(params.key);
      } else {
        return o.name === params.key;
      }
    })[0] || type;

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

      if (bucket.content >= count) {
        bucket.lastConformant = true;
        bucket.content -= count;
      } else {
        bucket.lastConformant = false;
      }

      bucket.reset = this._getResetTimestamp(bucket, typeParams);

      return bucket;
    }, (err, bucket) => {
      if (err) { return callback(err); }
      callback(null, {
        conformant: bucket.lastConformant,
        remaining:  bucket.content,
        reset:      bucket.reset,
        limit:      typeParams.size
      });
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

    const typeParams = type.overrides.filter(o => {
      if (o.match) {
        return o.match.exec(params.key);
      } else {
        return o.name === params.key;
      }
    })[0] || type;

    const count = params.count || typeParams.size;

    type.db.gms(params.key, (bucket) => {
      if (bucket && typeof bucket === 'string') {
        bucket = JSON.parse(bucket);
      }
      bucket.content = Math.min(typeParams.size, bucket.content + count);
      return bucket;
    }, (err, bucket) => {
      if (err) {
        return callback(err);
      }
      callback(null, {
        remaining:  bucket.content,
        reset:      bucket.reset,
        limit:      typeParams.size
      });
    });
  }
}


module.exports = LimitDB;
