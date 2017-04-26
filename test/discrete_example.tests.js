const LimitDB  = require('../lib/db');
const MockDate = require('mockdate');
const assert   = require('chai').assert;

const types = {
  ip: {
    size:         5,
    per_interval: 5,
    interval:     500,
    discrete:     true
  }
};

describe('when the fill rate is discrete', () => {
  afterEach(function () {
    MockDate.reset();
  });

  const db = new LimitDB({
    inMemory: true,
    types
  });

  beforeEach(function(done) {
    MockDate.set(Date.now());
    db.take({ type: 'ip', key: '21.17.65.41', count: 5 }, done);
  });

  it('should not add less than the per_interval amount', function(done) {
    MockDate.set(Date.now() + 150);
    db.status({ type: 'ip', prefix: '21.17.65.41' }, (err, result) => {
      if (err) { return done(err); }
      assert.equal(result.items[0].remaining, 0);
      done();
    });
  });

  it('should add the per_interval amount after the elapsed interval', function(done) {
    MockDate.set(Date.now() + 500);
    db.status({ type: 'ip', prefix: '21.17.65.41' }, (err, result) => {
      if (err) { return done(err); }
      assert.equal(result.items[0].remaining, 5);
      done();
    });
  });
});
