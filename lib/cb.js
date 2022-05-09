// Based on the work of Jeremy Martin: https://github.com/jmar777/cb
// Added a second timeout(1) to force spinning again the event loop lap and verify if the initial operation has been successful
module.exports = function(callback) {

  var cb = function() {
      if (timedout || (once && count)) return;
      count += 1;
      tid && clearTimeout(tid);

      var args = Array.prototype.slice.call(arguments);
      process.nextTick(function() {
        if (!errback) return callback.apply(this, args);
        args[0] ? errback(args[0]) : callback.apply(this, args.slice(1));
      });

    }, count = 0, once = false, timedout = false, errback, tid;

  cb.timeout = function(ms) {
    tid && clearTimeout(tid);
    tid = setTimeout(function() {
      // force another second timeout to verify if the operation has been successful
      // No need to clear timeout since it has been triggered
      tid = setTimeout(function() {
        cb(new TimeoutError(ms));
        timedout = true;
      }, 1);
    }, ms - 1);
    return cb;
  };

  cb.error = function(func) { errback = func; return cb; };

  cb.once = function() { once = true; return cb; };

  return cb;

};

var TimeoutError = module.exports.TimeoutError = function TimeoutError(ms) {
  this.message = 'Specified timeout of ' + ms + 'ms was reached';
  Error.captureStackTrace(this, this.constructor);
};
TimeoutError.prototype = new Error;
TimeoutError.prototype.constructor = TimeoutError;
TimeoutError.prototype.name = 'TimeoutError';