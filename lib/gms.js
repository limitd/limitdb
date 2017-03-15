const clone = require('fast-clone');

/**
 * GET->MAP->SAVE is an extension for leveldb.
 *
 * The idea is to take advantage of the single-threaded nature of node.js
 * to create an atomic operation but at the same time reducing the number of
 * rounds to leveldb.
 *
 *  The signature of the function is (key, map, callback).
 *
 * - key is the key of the leveldb element we want to retrieve
 * - map is a **synchronous** function to transform the value
 * - calback is executed after the change has been stored in the database
 *
 * var db = gms(db);
 *
 * db.gms('test', v => (v || 0) + 1, (err, v) => console.log(`value is ${v}`));
 * db.gms('test', v => (v || 0) + 1, (err, v) => console.log(`value is ${v}`));
 * db.gms('test', v => (v || 0) + 1, (err, v) => console.log(`value is ${v}`));
 *
 * will print:
 *
 *   value is 1
 *   value is 2
 *   value is 3
 *
 * The value stored in the database is 3 and the most important thing
 * this has executed **1 db.get** and **1 db.put**.
 *
 */
module.exports = function(db) {
  const queues = {};

  db.gms = function(key, map, putParams, callback) {
    const queue = queues[key] = queues[key] || [];

    queue.push({ map, putParams, callback });

    if (queue.length > 1){
      return;
    }

    this.get(key, (err, value) => {
      if (err && err.name !== 'NotFoundError') {
        delete queues[key];
        return queue.forEach(queued => queued.callback(err));
      }

      const intermediateResults = [];

      const finalResult = queue.reduce((prev, queued) => {
        const result = queued.map(clone(prev));
        intermediateResults.push(result);
        return result;
      }, value);

      delete queues[key];

      const last = queue.slice(-1)[0];

      this.put(key, finalResult, last.putParams, (err) => {
        if (err) {
          queue.forEach(queued => queued.callback(err));
        }
        intermediateResults.forEach((ir, index) => queue[index].callback(null, ir));
      });
    });
  };

  return db;
};
