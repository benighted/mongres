var SYNCS_PATH = './syncs/';
var DB_TYPE_MONGODB = 'mongodb';
var DB_TYPE_POSTGRESQL = 'postgresql';

var fs = require('fs');
var mg = require('mongodb').Db;
var pg = require('pg');
require('pg-parse-float')(pg);

var log = function (msg) {
  console.log(msg);
};

var error = function (err) {
  if (err) console.error(err);
};

var connect = function (config, callback) {
  if (!callback) { // valid client callback is required
    return error("Callback is not defined.");
  } else if (!config) { // check database configuration
    return callback("Database config is not defined.");
  } else if (!config.type) {
    return callback("Database type is not defined.");
  } else if (!config.host) {
    return callback("Database host is not defined.");
  } else if (!config.name) {
    return callback("Database name is not defined.");
  }

  var url = null;
  switch (config.type.toLowerCase()) {
    case "pg":
    case "postgres":
    case "postgresql":
    case DB_TYPE_POSTGRESQL:
      config.type = DB_TYPE_POSTGRESQL; // standardize database type
      log("Connecting to PostgreSQL server at " + config.host + "/" + config.name + "...");
      url = 'tcp://' +
        (config.user ? config.user + (config.pass ? ':' + config.pass : '') + '@' : '') +
        (config.host + (config.port ? ':' + config.port : '') + '/' + config.name);
      pg.connect(url, function(err, client, close) {
        if (err) return callback(err);
        log("Connected to PostgreSQL server at " + config.host + "/" + config.name + ".");
        callback(err, client, function () {
          log("Closing PostgreSQL client for " + config.host + "/" + config.name + "...");
          close();
        });
      });
      break;
    case "mg":
    case "mongo":
    case "mongodb":
    case DB_TYPE_MONGODB:
      config.type = DB_TYPE_MONGODB; // standardize database type
      log("Connecting to MongoDB server at " + config.host + "/" + config.name + "...");
      url = "mongodb://" + config.host + ":" + config.port + "/" + config.name;
      mg.connect(url, function(err, client) {
        if (err) return callback(err);
        log("Connected to MongoDB server at " + config.host + "/" + config.name + ".");
        callback(err, client, function () {
          log("Closing MongoDB client for " + config.host + "/" + config.name + "...");
          client.close();
        });
      });
      break;
    default:
      return end("Invalid source type specified.");
  }
};

var run = function (sync, callback) {
  if (sync instanceof Array) { // run the elements in sequence
    if (sync.length) run(sync.shift(), function (err) {
      if (err) callback(err);
      else run(sync, callback);
    }); else callback();
    return;
  }

  var startDate = new Date();
  var finishDate = null;

  // default generic error handler
  if (!callback) callback = error;

  if (!sync.source) {
    return callback("Source is not defined.");
  } else if (!sync.target) {
    return callback("Target is not defined.");
  } else if (!sync.operations || !sync.operations.length) {
    return callback("Operations are not defined.");
  }

  log("Operations starting at " + startDate);
  // log(JSON.stringify(sync, null, "  "));

  // supplement the given callback
  var endOps = []; // terminations
  var end = function (err) {
    // run any termination operations
    while (endOps && endOps.length) {
      var op = endOps.shift();
      if (typeof op == 'function') op();
      else log(op);
    }
    // send stats to the log
    finishDate = new Date();
    log("\nOperations finished at " + finishDate);
    log("Elapsed time: " + ((finishDate.getTime() - startDate.getTime()) / 1000) + " seconds.");
    if (callback) callback(err);
  };

  connect(sync.source, function (err, client, close) {
    if (close) endOps.push(close);
    if (client) sync.source.client = client;
    if (err) return end(err); // source errors
    connect(sync.target, function (err, client, close) {
      if (close) endOps.push(close);
      if (client) sync.target.client = client;
      (function run(err) { // run each op if no error
        if (err || !sync.operations.length) end(err);
        else runOp(sync.source, sync.target, sync.operations.shift(), run);
      })(err);
    });
  });
};

var buildCounter = function () {
  return {
    _readCount: 0,
    _writeCount: 0,
    _errorCount: 0,
    _readCountTotal: 0,
    _writeCountTotal: 0,
    _errorCountTotal: 0,
    get readCount() {
      return this._readCount;
    },
    set readCount(n) {
      if (n < 0) return;
      this._readCountTotal += n - this._readCount;
      this._readCount = n;
    },
    get readCountTotal() {
      return this._readCountTotal;
    },
    get writeCount() {
      return this._writeCount;
    },
    set writeCount(n) {
      if (n < 0) return;
      this._writeCountTotal += n - this._writeCount;
      this._writeCount = n;
    },
    get writeCountTotal() {
      return this._writeCountTotal;
    },
    get errorCount() {
      return this._errorCount;
    },
    set errorCount(n) {
      if (n < 0) return;
      this._errorCountTotal += n - this._errorCount;
      this._errorCount = n;
    },
    get errorCountTotal() {
      return this._errorCountTotal;
    },
    // reset counts but leave totals intact
    reset: function () {
      this._readCount = 0;
      this._writeCount = 0;
      this._errorCount = 0;
    }
  };
};

var buildUpdater = function (target, op) {
  var updater = null;

  if (op.target.type === DB_TYPE_POSTGRESQL) {
    // not implemented yet
  } else if (target.type === DB_TYPE_MONGODB) {
    var collection = target.client.collection(op.target);
    updater = function (data, callback) {
      // default generic error handler
      if (!callback) callback = error;

      // a function to dereference data models
      var deref = function deref(model, src) {
        var doc = {};
        for (var k in model) {
          if (typeof model[k] === 'object') {
            doc[k] = deref(model[k], src);
          } else if (typeof model[k] === 'string') {
            if (model[k] in src) doc[k] = src[model[k]];
          }
        }
        return doc;
      };

      // get values to use for query
      var query = deref(op.query, data);
      // get values to use for update
      var update = deref(op.update, data);

      // verify values are dereferenced
      if (!Object.keys(query).length) {
        callback('Query values not found in source.');
      } else if (!Object.keys(update).length) {
        callback('Update values not found in source.');
      } else { // update target collection
        collection.update(query, update, {
          multi: true,
          upsert: true,
          journal: true,
          w: 1
        }, callback);
      }
    };
  }

  return updater;
};

var interpolate = function (text, data) {
  if (!data || typeof data != 'object') {
    // invalid or missing data source
  } else if (typeof text == 'object') {
    for (var key in text) {
      text[key] = interpolate(text[key], data);
    }
  } else if (typeof text == 'string') {
    var matches = text.match(/{{([^{}]*)}}/gm);
    for (var i = 0; matches && i < matches.length; i++) {
      if (data[matches[i].slice(2, -2)]) { // replace handlebars tokens
        text = text.replace(matches[i], data[matches[i].slice(2, -2)]);
      }
    }
  }
  return text;
};

var buildReader = function (source, op, updater) {
  var reader = null, counter = buildCounter(), initialized = {};

  if (source.type === DB_TYPE_POSTGRESQL) {
    // get list of columns needed for query
    var columns = [];
    (function getCols(model) {
      for (var k in model) {
        if (typeof model[k] === 'object') {
          getCols(model[k]);
        } else if (typeof model[k] === 'string') {
          columns.push('"' + model[k] + '"');
        }
      }
    })([op.query, op.update]);

    // build base query for the operation
    var sql = "SELECT " + columns.join(',') + " FROM " + op.source +
      (op.filter ? " WHERE " + op.filter : "") +
      (!op.cursor && op.limit ? " LIMIT " + (op.limit + 1) : "") +
      (!op.cursor && op.limit && op.offset ? " OFFSET " + op.offset : "");

    // build a reader function for source
    reader = function (callback) {
      // perform initialization action if defined and not initialized
      if (op.actions && op.actions.init && !initialized.initAction) {
        log("\nExecuting 'init' query '" + op.actions.init + "'...");
        return source.client.query(op.actions.init, function (err, result) {
          initialized.initAction = true;
          return reader(callback);
        });
      }

      // open cursor if defined and not initialized
      if (op.cursor && !initialized.sourceCursor) {
        op.cursor = op.source.replace(/[^a-z0-9_]/gi,'_') + "_cursor_" + new Date().getTime();
        sql = "DECLARE " + op.cursor + " NO SCROLL CURSOR WITH HOLD FOR (" + sql + ")";
        log("\nOpening cursor '" + op.cursor + "'...");
        return source.client.query(sql, function (err, result) {
          if (err) return callback(err);
          // replace original sql query with a query against this cursor
          sql = "FETCH FORWARD " + (op.limit ? op.limit : 1000) + " FROM " + op.cursor;
          initialized.sourceCursor = true;
          return reader(callback);
        });
      }

      // execute the query and repeat as is necessary for results
      log("\nExecuting query '" + sql + "'...");
      if (op.cursor) log("Using cursor '" + op.cursor + "'...");
      var err, query = source.client.query(sql, function (err2) {
        if (err2) err = err2; // record error for the "end" event
      });
      query.on('row', function (row) {
        // honor the operation read limit if one has been set
        if (op.limit && counter.readCount >= op.limit) return;
        else {
          ++counter.readCount;
          updater(row, function (err, aff) {
            if (err || !aff) {
              ++counter.errorCount;
              if (err) error (err);
            } else ++counter.writeCount;
            log("R: " + counter.readCount + " / " + counter.readCountTotal +
              "\tW: " + counter.writeCount + " / " + counter.writeCountTotal +
              "\tE: " + counter.errorCount + " / " + counter.errorCountTotal);
          });
        }
      });
      query.on('end', function end(result) {
        if (!op.limit || counter.readCount < op.limit) {
          // finished reading rows from source
          if (op.cursor && initialized.cursor) {
            log("\nClosing cursor '" + op.cursor + "'...");
            source.client.query("CLOSE " + op.cursor);
            initialized.cursor = false;
          }
        }
        if (counter.readCount) { // check readCount to avoid divide-by-zero errors when no records were returned
          log((((counter.writeCount + counter.errorCount) / counter.readCount) * 100).toFixed(1) + "% written...");
        }
        if (counter.readCount > (counter.writeCount + counter.errorCount)) {
          setTimeout(end, 500); // wait for database writes to conclude
        } else {
          if (!err && op.limit && counter.readCount >= op.limit) {
            // reset the counter and run another batch
            op.offset = op.offset + counter.readCount;
            counter.reset();
            return reader(callback);
          } else {
            // perform finalization action if defined
            if (op.actions && op.actions.done && !err && !counter.errorCountTotal) {
              log("\nExecuting 'done' query '" + op.actions.done + "'...");
              return source.client.query(op.actions.done, function (err) {
                return callback(err);
              });
            } else if (op.actions && op.actions.fail && (err || counter.errorCountTotal)) {
              log("\nExecuting 'fail' query '" + op.actions.fail + "'...");
              return source.client.query(op.actions.fail, function (err) {
                return callback(err);
              });
            } else return callback(err);
          }
        }
      });
    };
  } else if (source.type === DB_TYPE_MONGODB) {
    // not implemented yet
  }

  return reader;
};

var runOp = function (source, target, op, callback) {
  // default generic error handler
  if (!callback) callback = error;

  // output op json to log
  log("\nStarting operation...");
  log(JSON.stringify(op, null, "  "));

  // function to update target database
  var updater = buildUpdater(target, op);
  if (!updater) return callback('Target is invalid.');

  // function to read data into updater function
  var reader = buildReader(source, op, updater);
  if (!reader) return callback('Source is invalid.');

  // begin reading if objects were created
  return reader(callback);
};

// loop through sync definitions in SYNCS_PATH
fs.readdir(SYNCS_PATH, function (err, files) {
  if (err) return log(err);

  log("Sync starting...");
  var end = function () {
    log("Sync finished.");
    pg.end(); // terminate sessions
  };

  (function readLoop(files) {
    if (!files || !files.length) end();
    var file = files.shift();
    log("Reading from '" + file + "'...");
    var sync = require(SYNCS_PATH + file);
    if (!sync) error("Invalid file: " + file);
    else run(sync, error);
    if (files.length) readLoop(files);
    else end();
  })(files);
});
