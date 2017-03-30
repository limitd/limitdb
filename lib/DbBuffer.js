'use strict';

const ms = require('ms');
const async = require('async');
const _ = require('lodash');

//inspired by https://github.com/dominictarr/hashlru

class DbBuffer {
  constructor(params) {
    this.max = 5000;
    this.size = 0;
    this.current = Object.create(null);
    this.previous = Object.create(null);

    this._db = params.db;

    const flushInterval = typeof params.flushInterval === 'string' ?
                            ms(params.flushInterval):
                            params.flushInterval;

    this._setFlushInterval(flushInterval || ms('5m'));

    this.flushed = false;
    this.locks = {};
  }

  _setFlushInterval(flushInterval) {
    setTimeout(() => {
      this.flush(() => {
        this._setFlushInterval(flushInterval);
      });
    }, flushInterval);
  }

  _flushStage(stage, done) {
    async.forEach(Object.keys(stage), (key, saved) => {
      this._db.put(key, stage[key], saved);
    }, done);
  }

  flush(callback) {
    this.flushed = true;

    async.parallel([
      done => this._flushStage(this.current, done),
      done => this._flushStage(this.previous, done)
    ], callback || _.noop);
  }

  _update(key, value) {
    this.current[key] = value;
    this.size++;

    if(this.size >= this.max) {
      this.flush();
      this.size = 0;
      this.previous = this.current;
      this.current = Object.create(null);
    }
  }

  _set(key, value) {
    if(this.current[key] !== undefined) {
      this.current[key] = value;
    } else {
      this._update(key, value);
    }
    return value;
  }

  _get(key) {
    const fromCurrent = this.current[key];

    if(fromCurrent !== undefined) {
      return fromCurrent;
    }

    const fromPrevious = this.previous[key];

    if(fromPrevious) {
      this._update(key, fromPrevious);
      return fromPrevious;
    }
  }

  lock(key, run) {
    if (!this.locks[key]) {
      this.locks[key] = [];
      run(() => {
        const waiting = this.locks[key];
        var cb;
        while((cb = waiting.shift())) {
          cb(_.noop);
        }
        this.locks[key] = false;
      });
    } else {
      this.locks[key].push(run);
    }
  }

  get(key, typeParams, callback) {
    this.flushed = false;

    this.lock(key, release => {
      const fromBuffer = this._get(key);
      if (fromBuffer) {
        callback(null, fromBuffer);
        return release();
      }

      this._db.get(key, (err, bucket) => {
        if (err && err.name !== 'NotFoundError') {
          callback(err);
          return release();
        }

        if (typeof bucket === 'string') {
          bucket = JSON.parse(bucket);
        } else if (typeof bucket === 'undefined') {
          bucket = {
            lastDrip: Date.now(),
            content: typeParams.size,
            size: typeParams.size
          };
        }

        this._set(key, bucket);
        callback(null, bucket);
        release();
      });
    });
  }
}

module.exports = DbBuffer;
