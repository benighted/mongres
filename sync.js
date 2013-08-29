var SYNCS_PATH = "./syncs/";
var DB_TYPE_MONGODB = "mongodb";
var DB_TYPE_POSTGRESQL = "postgresql";

var fs = require("fs");
var mg = require("mongodb").Db;
var pg = require("pg");
var ansi = require("ansi");
console.cursor = ansi(process.stdout);

var debugMode = false;
var canParseFloat = true;
var syncFiles = [];

if (process && process.argv) {
  for (var i = 2; i < process.argv.length; i++) {
    switch (process.argv[i]) {
      case "-d":
      case "--debug":
        debugMode = true;
        break;
      case "-npf":
      case "--no-parse-float":
        canParseFloat = false;
        break;
      case "-s":
      case "--syncs":
      case "--syncs-path":
        // take next argument as a directory path and increment iterator
        if (process.argv[i + 1]) {
          SYNCS_PATH = process.argv[++i].trim();
          if (SYNCS_PATH.charAt(SYNCS_PATH.length - 1) != "/") SYNCS_PATH += "/";
        }
        break;
      case "-f":
      case "--file":
      case "--files":
        // take next argument as a file name or list and increment iterator
        var fileNames = process.argv[++i].split(",");
        for (j = 0; j < fileNames.length; j++) {
          syncFiles.push(fileNames[j].trim());
        }
        break;
      default:
        // take unknown argument as a file name if not starting with dash (-)
        if (process.argv[i].charAt(0) != "-") syncFiles.push(process.argv[i].trim());
        break;
    }
  }
}

var log = function (msg) {
  console.log(msg);
};

var debug = function (msg) {
  if (debugMode) {
    console.cursor.yellow();
    console.info("[ DEBUG ] " + msg);
    console.cursor.reset();
  }
};

var error = function (err) {
  if (err) {
    console.cursor.red().bold();
    console.error("[ ERROR ] " + err);
    console.cursor.reset();
  }
};

var interpolate = function (text, data) {
  if (!data || typeof data != "object") {
    // invalid or missing data source
  } else if (typeof text == "object") {
    for (var key in text) {
      text[key] = interpolate(text[key], data);
    }
  } else if (typeof text == "string") {
    var matches = text.match(/{{([^{}]*)}}/gm);
    for (var i = 0; matches && i < matches.length; i++) {
      if (data[matches[i].slice(2, -2)]) { // replace handlebars tokens
        text = text.replace(matches[i], data[matches[i].slice(2, -2)]);
      }
    }
  }
  return text;
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
          if (typeof model[k] === "object") {
            doc[k] = deref(model[k], src);
          } else if (typeof model[k] === "string") {
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
        callback("Query values not found in source.");
      } else if (!Object.keys(update).length) {
        callback("Update values not found in source.");
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

var buildReader = function (source, op, updater) {
  var reader = null, counter = buildCounter(), initialized = {};

  if (source.type === DB_TYPE_POSTGRESQL) {
    // get list of columns needed for query
    var columns = [];
    (function getCols(model) {
      for (var k in model) {
        if (typeof model[k] === "object") {
          getCols(model[k]);
        } else if (typeof model[k] === "string") {
          columns.push('"' + model[k] + '"');
        }
      }
    })([op.query, op.update]);

    // build base query for the operation
    var sql = "SELECT " + columns.join(",") + " FROM " + op.source +
      (op.filter ? " WHERE " + op.filter : "") +
      (!op.cursor && op.limit ? " LIMIT " + (op.limit + 1) : "") +
      (!op.cursor && op.limit && op.offset ? " OFFSET " + op.offset : "");

    // build a reader function for source
    reader = function (callback) {
      // perform initialization action if defined and not initialized
      if (op.actions && op.actions.init && !initialized.initAction) {
        debug("Executing 'init' query '" + (op.actions.init.text || op.actions.init) + "'...");
        return source.client.query(op.actions.init, function (err, result) {
          if (err) return callback(err);
          initialized.initAction = true;
          return reader(callback);
        });
      }

      // open cursor if defined and not initialized
      if (op.cursor && !initialized.sourceCursor) {
        // generate randomish cursor name if not set
        if (typeof op.cursor != "string") op.cursor = op.source.replace(/[^a-z0-9_]+/gi,"_") + "_cursor_" + new Date().getTime();
        // open the cursor and prepare to fetch from it
        sql = "DECLARE " + op.cursor + " NO SCROLL CURSOR WITH HOLD FOR (" + sql + ")";
        debug("Opening cursor '" + op.cursor + "'...");
        return source.client.query(sql, function (err, result) {
          if (err) return callback(err);
          // replace original sql query with a query against this cursor
          sql = "FETCH FORWARD " + (op.limit ? op.limit : 1000) + " FROM " + op.cursor;
          initialized.sourceCursor = true;
          return reader(callback);
        });
      }

      // execute the query and repeat as is necessary for results
      debug("Executing query '" + sql + "'...");
      var err, query = source.client.query(sql, function (err2) {
        if (err2) err = err2; // record error for the "end" event
      });
      query.on("row", function (row) {
        // honor the operation read limit if one has been set
        if (op.limit && counter.readCount >= op.limit) return;
        else {
          ++counter.readCount;
          updater(row, function (err, aff) {
            if (err || !aff) {
              ++counter.errorCount;
              if (err) error (err);
            } else ++counter.writeCount;
            debug("R: " + counter.readCount + " / " + counter.readCountTotal +
              "\tW: " + counter.writeCount + " / " + counter.writeCountTotal +
              "\tE: " + counter.errorCount + " / " + counter.errorCountTotal);
          });
        }
      });
      query.on("end", function end(result) {
        if (!op.limit || counter.readCount < op.limit) {
          // finished reading rows from source
          if (op.cursor && initialized.sourceCursor) {
            debug("Closing cursor '" + op.cursor + "'...");
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
              debug("Executing 'done' query '" + (op.actions.done.text || op.actions.done) + "'...");
              return source.client.query(op.actions.done, callback);
            } else if (op.actions && op.actions.fail && (err || counter.errorCountTotal)) {
              debug("Executing 'fail' query '" + (op.actions.fail.text || op.actions.fail) + "'...");
              return source.client.query(op.actions.fail, callback);
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
      url = "postgres://" +
        (config.user ? config.user + (config.pass ? ":" + config.pass : "") + "@" : "") +
        (config.host + (config.port ? ":" + config.port : "") + "/" + config.name);
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
  debug(JSON.stringify(sync, null, "  "));

  // supplement the given callback
  var endOps = []; // terminations
  var end = function (err) {
    // run any termination operations
    while (endOps && endOps.length) {
      var op = endOps.shift();
      if (typeof op == "function") op();
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

var runOp = function (source, target, op, callback) {
  var startDate = new Date();
  var finishDate = null;

  log("\n" + (op.name ? op.name : "Operation") + " starting at " + startDate + "...");
  debug(JSON.stringify(op, null, "  "));

  // default generic error handler
  if (!callback) callback = error;
  // supplement the given callback
  var endOp = function (err) {
    // send stats to the log
    finishDate = new Date();
    log("\n" + (op.name ? op.name : "Operation") + " finished at " + finishDate + "...");
    log("Elapsed time: " + ((finishDate.getTime() - startDate.getTime()) / 1000) + " seconds.");
    if (callback) callback(err);
  };

  // function to update target database
  var updater = buildUpdater(target, op);
  if (!updater) return endOp("Target is invalid.");

  // function to read data into updater function
  var reader = buildReader(source, op, updater);
  if (!reader) return endOp("Source is invalid.");

  // call reader() to begin
  return reader(endOp);
};

// execution begins here

log("Sync starting...");
if (debugMode) log("Debug mode enabled.");
if (canParseFloat) require("pg-parse-float")(pg);

// loop through sync definitions in SYNCS_PATH
fs.readdir(SYNCS_PATH, function (err, files) {
  if (err) return error(err);

  (function readLoop(files, callback) {
    if (!files || !files.length) callback("No files found.");
    var fileName = files.shift().trim();
    debug(fileName.indexOf(".json") + " / " + fileName.length);
    if (fileName.length < 5 || fileName.lastIndexOf(".json") != fileName.length - 5) {
      debug("Skipping file '" + fileName + "'..."); // skip it
    } else if (syncFiles.length && syncFiles.indexOf(fileName) === -1) {
      debug(syncFiles);
      debug("Skipping file '" + fileName + "'..."); // skip it
    } else try {
      debug("Reading from '" + fileName + "'...");
      var sync = require(SYNCS_PATH + fileName);
      if (!sync) throw "Unable to parse JSON data.";
      else run(sync, callback);
    } catch (err) {
      error("Invalid file: " + fileName);
      error(err);
    }
    // run next file(s) concurrently
    if (files.length) readLoop(files, callback);
    else callback();
  })(files, function (err) {
    if (err) error(err);
    log("Sync finished.");
    pg.end(); // terminate sessions
  });

});
