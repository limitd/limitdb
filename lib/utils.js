const ms     = require('ms');
const _      = require('lodash');
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
    'unlimited'
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

/**
 * Load the buckets configuration.
 *
 * @param {Object.<string, type>} bucketsConfig The buckets configuration.
 * @memberof LimitDB
 */
function buildBuckets(bucketsConfig) {
  return _.reduce(bucketsConfig, (result, bucket, name) => {
    result[name] = normalizeType(bucket);
    return result;
  }, {});
}

function buildBucket(bucket) {
  return normalizeType(bucket);
}

class LimitdRedisValidationError extends Error {
  constructor(msg, extra) {
    super();
    this.name = this.constructor.name;
    this.message = msg;
    Error.captureStackTrace(this, this.constructor);
    if (extra) {
      this.extra = extra;
    }
  }
}

module.exports = {
  buildBuckets,
  buildBucket,
  LimitdRedisValidationError
};
