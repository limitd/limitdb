const tmp = require('tmp');
const LimitDB  = require('../lib/db');
const assert   = require('chai').assert;

/**
 * To run this test you need a big broken database.
 */

const describeOrSkip = process.env.BIG_BROKEN_DB ?
  describe :
  describe.skip.bind(describe);

describeOrSkip('Repair Database', () => {
  var db, start;

  before(function() {
    db = new LimitDB({
      path: process.env.BIG_BROKEN_DB,
      types: {
        ip: { per_second: 5 }
      }
    });

    start = Date.now();
  });

  it('should emit repairing after ~5s and then ready', function(done) {
    db.once('repairing', () => {
      assert.approximately(Date.now() - start, 5000, 100);
      db.once('ready', () => {
        assert.ok(db.isOpen());
        done();
      });
    });
  });
});

describe('repair db 2', function() {
  var db, start;

  before(function() {
    db = new LimitDB({
      path: tmp.dirSync().name,
      types: {
        ip: { per_second: 5 }
      }
    });

    start = Date.now();
  });

  it('should not emit repairing for empty path', function(done) {
    db.once('ready', done);
    db.once('repairing', () => {
      done(new Error('this should not be triggered'));
    });
  });
});

