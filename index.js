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

Mongres.prototype.run = function (callback) {
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
  async.eachSeries(queue, function (op, next) {
    op.run(function (err) {
      op.finish(next.bind(this, err));
    });
  }, callback);
};

module.exports = Mongres;
