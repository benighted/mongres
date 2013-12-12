var util = require('util');
var async = require('async');

var Database = require('./Database');

function Operation(config) {
  if (!config.db)           throw new Error('Database definition is required.');
  if (!config.op)           throw new Error('Operation definition is required.');
  if (!config.op.extract)   throw new Error('Extract function is required.');
  if (!config.op.load)      throw new Error('Load function is required.');

  if (config.debug) console.info('Debug mode enabled.');

  // transforms are optional, ensure the node exists
  if (!config.op.transform) config.op.transform = {};

  this.db = {};
  this.op = config.op;
  this.debug = config.debug;

  // construct database objects
  for (var db in config.db) {
    // use object key for alias
    if (!config.db[db].alias) {
      config.db[db].alias = db;
    }

    // inherit debug mode from the config
    if (config.db[db].debug === undefined) {
      config.db[db].debug = config.debug;
    }

    this.db[db] = new Database(config.db[db]);
  }

  // validate and standardize the operation steps
  var steps = ['init','extract','load','exit'];
  for (var i in steps) for (var name in this.op[steps[i]]) {
    if (!this.db[name]) {
      throw new Error('Undefined database for ' + steps[i] + ': ' + name);
    } else if (!this.op[steps[i]][name]) {
      throw new Error('Invalid function for ' + steps[i] + ': ' + name);
    }

    // wrap all steps in arrays for compatibility
    if (!util.isArray(this.op[steps[i]][name])) {
      this.op[steps[i]][name] = [this.op[steps[i]][name]];
    }
  }
}

Operation.prototype.run = function (callback) {
  if (!callback) callback = function () {};

  var self = this;
  var queue = [];
  var registry = {
    startDate: new Date()
  };

  // queue all db connections
  for (var name in self.db) {
    queue.push(self.db[name].connect.bind(self.db[name]));
  }

  // queue init function(s)
  for (var name in self.op.init) for (var i in self.op.init[name]) {
    if (self.debug) queue.push(function (next) {
      console.log((self.op.name || 'Operation') + ' : init');
      next();
    });

    queue.push(self.op.init[name][i].bind(self, self.db[name], registry));
  }

  // queue extract, transform, and load functions
  for (var name in self.op.extract) for (var i in self.op.extract[name]) {
    if (self.debug) queue.push(function (next) {
      console.log((self.op.name || 'Operation') + ' : extract > transform > load');
      next();
    });

    // preserve variables for closure
    var source = name, sourceIdx = i;

    queue.push(function (next) {
      var reads = 0, writes = 0,
        start = null, ending = false,
        done = function done(err) {
          if (ending) return;
          if (err || reads === writes) {
            ending = true;
            return next(err);
          } else { // wait for loading
            return setTimeout(done, 100);
          }
        },
        process = function process(data, cb) {
          reads++;
          if (!start) start = new Date(); // first record is read
          if (self.debug) console.log((self.op.name || 'Operation') +
            ' : READ ' + reads);

          // transform data if function is defined
          if (self.op.transform[source]) {
            data = self.op.transform[source](self.db[source], registry, data);
          }

          // queue loader functions
          var loaders = [];
          for (var target in self.op.load) for (var j in self.op.load[target]) {
            loaders.push(
              self.op.load[target][j].bind(self, self.db[target], registry, data)
            );
          }

          // execute loader functions in series
          async.series(loaders, function (err) {
            if (err) return cb(err) + done(err);

            writes++;
            if (self.debug) console.log((self.op.name || 'Operation') +
              ' : LOAD ' + writes + ' ' + ((writes/reads) * 100).toFixed(2) + '% ' + 
              (writes / ((new Date().getTime() - start.getTime()) / 1000)).toFixed(1) + ' r/s');

            return cb();
          });
        };

      return self.op.extract[source][sourceIdx](
        self.db[source], registry, process, done
      );
    });
  }

  // queue exit function(s)
  for (var name in self.op.exit) for (var i in self.op.exit[name]) {
    if (self.debug) queue.push(function (next) {
      console.log((self.op.name || 'Operation') + ' : exit');
      next();
    });

    queue.push(self.op.exit[name][i].bind(
      self, self.db[name], registry
    ));
  }

  // execute queued functions in series
  console.info((self.op.name || 'Operation') + ' starting at ' + new Date());
  return async.series(queue, function (err) {
    console.info((self.op.name || 'Operation') + ' finished at ' + new Date());
    callback(err);
  });
};

Operation.prototype.finish = function (callback) {
  var queue = [];

  // queue close function(s)
  for (var db in this.db) {
    queue.push(this.db[db].close.bind(this.db[db]));
  }

  // execute queued functions in parallel
  return async.parallel(queue, callback);
};

module.exports = Operation;
