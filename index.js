var util = require('util');
var async = require('async');

var Database = require('./lib/Database');
var Operation = require('./lib/Operation');

var Mongres = function Mongres(config) {
  // wrap op in array for compatibility
  if (!util.isArray(config.op)) {
    config.op = [config.op];
  }

  this.config = config;
};

Mongres.prototype.run = function () {
  var queue = [];

  // queue defined operations
  for (var i in this.config.op) {
    queue.push(new Operation({
      db: this.config.db,
      op: this.config.op[i],
      debug: this.config.debug || this.config.op.debug
    }));
  }

  // run operations in series
  console.info('Running operations...');
  async.eachSeries(queue, function (op, next) {
    op.run(function (err) {
      if (err) console.error(err);
      op.finish(next);
    });
  }, function (err) {
    if (err) return console.error(err);
    console.info('All operations have finished.');
  });
};

module.exports = Mongres;
