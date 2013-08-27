var SYNCS_PATH = './syncs/';
var DB_TYPE_MONGODB = 'mongodb';
var DB_TYPE_POSTGRESQL = 'postgresql';

var mg = require('mongodb').Db;
var pg = require('pg');
var fs = require('fs');

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
    if (sync.length) return run(sync.shift(), function (err) {
      if (err) callback(err);
      else run(sync, callback);
    });
    return callback();
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
    log("Operations finished at " + finishDate);
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

/*

  postgres source data:
  {
    "column1": "value",
    "column2": "value",
    ...
  }

  postgres target data:
  {
    "column1": "value",
    "column2": "value",
    ...
  }

  mongo source data:
  {
    "field1": "value",
    "field2": {
      "sub1": "value",
      "sub2": {
        "sub3": "value"
      }
    }
  }

  mongo target data:
  {
    "$set": {
      "field1": "value",
      "field2.sub1": "value",
      "field2.sub2": {"sub3": "value"},
      ...
    }
  }

 */
var runOp = function (source, target, op, callback) {
  var updateFunction = null, updateCallback = null, collections = {};
  var readCountTotal = 0, writeCountTotal = 0, errorCountTotal = 0;

  // default generic error handler
  if (!callback) callback = error;

  // output op json to log
  log(JSON.stringify(op));

  // build update function which saves data to target
  if (target.type === DB_TYPE_POSTGRESQL) {
    callback('PostgreSQL target not supported yet.');
  } else if (target.type === DB_TYPE_MONGODB) {
    updateCallback = function (err, num) {
      // increment write or error counter
      if (err || !num) {
        this.errorCount++;
        errorCountTotal++;
        log("Error " + this.errorCount + " / " + errorCountTotal);
        this.errorCount = (this.readCount - this.writeCount);
        callback(err);
      } else {
        this.writeCount++;
        writeCountTotal++;
        log("Write " + this.writeCount + " / " + writeCountTotal);
      }
    };
    updateFunction = function (data) {
      // honor operation read limit if set
      if (op.limit && this.readCount >= op.limit) return;

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
        this.errorCount++;
        return updateCallback("Query keys not found in data source.");
      } else if (!Object.keys(update).length) {
        this.errorCount++;
        return updateCallback("Update keys not found in data source.");
      }

      // connect to target collection and save a reference for later
      if (!collections[op.target]) { // collection has not been used
        collections[op.target] = target.client.collection(op.target);
      }

      // perform updates if connected
      if (!collections[op.target]) {
        this.errorCount++;
        return updateCallback("Unable to connect to '" + op.target + "' collection.");
      } else {
        // increment read counter
        this.readCount++;
        readCountTotal++;
        log("Read  " + this.readCount + " / " + readCountTotal);
        collections[op.target].update(query, update, {
          multi: true,
          upsert: true,
          journal: true,
          w: 1
        }, updateCallback.bind(this));
      }
    };
  } else return callback("Target client is invalid.");

  // build readFunction which feeds to updateFunction
  if (source.type === DB_TYPE_POSTGRESQL) {
    // white list to filter dangerous characters
    var sqlFilter = /[^a-z0-9,'"()<>!=\-\._ ]/gi;
    // get list of columns needed for query
    var columns = [];
    (function getCols(model) {
      for (var k in model) {
        if (typeof model[k] === 'object') {
          getCols(model[k]);
        } else if (typeof model[k] === 'string') {
          columns.push('"' + model[k].replace(sqlFilter) + '"');
        }
      }
    })([op.query, op.update]);

    // build base query and readFunction
    var sql = "SELECT " + columns.join(',') +
      " FROM " + op.source.replace(sqlFilter, "") +
      (op.filter ? " WHERE " + op.filter.replace(sqlFilter, "") : "");
    var readFunction = function readFunction() {
      var counter = { readCount: 0, writeCount: 0, errorCount: 0 };
      var err, query = source.client.query(sql + (op.offset && !op.cursor ? " OFFSET " + op.offset : ''));
      query.on('row', updateFunction.bind(counter));
      query.on('error', function (err2) { err = err2; });
      query.on('end', function end(result) {
        if (counter.readCount) log( // check readCount to avoid divide-by-zero errors
          (((counter.writeCount + counter.errorCount) / counter.readCount) * 100).toFixed(1) + "% written..."
        );
        if (counter.readCount > (counter.writeCount + counter.errorCount)) {
          setTimeout(end, 200);
        } else {
          if (!err && op.limit && counter.readCount >= op.limit) {
            if (!op.offset) op.offset = 0;
            op.offset += op.limit;
            readFunction();
          } else {
            if (op.cursor) {
              log("Closing cursor '" + op.cursor + "' ...");
              source.client.query("CLOSE " + op.cursor, function (err2) { callback(err || err2); });
            } else callback(err);
          }
        }
      });
    };

    // execute query by calling readFunction()
    if (op.cursor) { // open new cursor if set
      op.cursor = op.source.replace(/[^a-z0-9_]/gi,'_') + "_cursor_" + new Date().getTime();
      sql = "DECLARE " + op.cursor + " NO SCROLL CURSOR WITH HOLD FOR (" + sql + ")";
      log("Opening cursor '" + op.cursor + "' ...");
      source.client.query(sql, function (err, result) {
        if (err) return callback(err);
        // replace operation sql with a query to this cursor
        log("Fetching from cursor '" + op.cursor + "' ...");
        sql = "FETCH FORWARD " + (op.limit ? op.limit : 1000) + " FROM " + op.cursor;
        readFunction(); // fetch from the cursor
      });
    } else { // limit and query without a cursor
      if (op.limit) sql = sql + " LIMIT " + (op.limit + 1);
      readFunction();
    }
  } else if (source.type === DB_TYPE_MONGODB) {
    callback('MongoDB source not supported yet.');
  } else return callback("Source client is invalid.");
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
