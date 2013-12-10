var pg = require('pg');
require("pg-parse-float")(pg);
// var pgCursor = require('pg-cursor');
var PgQueryStream = require('pg-query-stream');

var mg = require('mongodb');
var MgClient = mg.MongoClient;
var MgServer = mg.Server;

var DB_TYPE_MONGODB    = 'mongodb';
var DB_TYPE_POSTGRESQL = 'postgresql';

function Database(config) {
  if (!config.type) throw new Error('Database type is required');
  if (!config.name) throw new Error('Database name is required');
  if (!config.host) throw new Error('Database host is required');
  if (!config.port) throw new Error('Database port is required');

  // standardize config.type
  switch (config.type) {
    case 'pg':
    case 'postgres':
    case 'postgresql':
      config.type = DB_TYPE_POSTGRESQL;
      break;
    case 'mg':
    case 'mongo':
    case 'mongodb':
      config.type = DB_TYPE_MONGODB;
      break;
    default:
      throw new Error('Unknown database type: ' + config.type);
      break;
  }

  this.client = null;
  this.debug = config.debug;

  this.type = config.type;
  this.name = config.name;
  this.host = config.host;
  this.port = config.port;
  this.user = config.user;
  this.pass = config.pass;

  // automatically generate the alias if not defined in config
  this.alias = config.alias || config.type + '/' + config.name;
}

Database.prototype.connect = function (callback) {
  if (typeof callback !== 'function') {
    callback = function () {}; // noop
  }

  if (this.connecting) { // connection in progress, try again
    return setTimeout(this.connect.bind(this, callback), 100);
  } else if (this.client && this.connected) { // is connected
    return process.nextTick(function () {     // return async
      callback(null, this.client);
    });
  }

  var self = this;
  self.connecting = true;
  self.connected = false;
  if (self.debug) console.info('Connecting to ' + self.alias +
    ' at ' + self.host + ':' + self.port);

  switch (self.type) {
    case DB_TYPE_MONGODB:
      var mgServer = new MgServer(self.host, self.port, {
        auto_reconnect: true
      });
      var mgClient = new MgClient(mgServer, {
        w: 1,
        journal: true
      });

      mgClient.open(function(err, client) {
        self.connecting = false;

        if (err) {
          return mgClient.close(callback.bind(self, err));
        } else if (client) {
          var db = client.db(self.name);
          self.client = db;
          self.connected = true;
          if (self.debug) console.info('Connected to ' + self.alias +
            ' at ' + self.host + ':' + self.port);
          return callback(err, db);
        }
      });
      break;
    case DB_TYPE_POSTGRESQL:
      var config = {
        user: self.user,
        host: self.host,
        database: self.name
      };

      // include optional config parameters if defined 
      if (self.pass !== undefined) config.password = self.pass;
      if (self.port !== undefined) config.port = self.port;
      if (self.ssl  !== undefined) config.ssl = self.ssl;

      return pg.connect(config, function(err, client, done) {
        self.connecting = false;

        if (client) {
          self.client = client;
          self.connected = true;
          if (self.debug) console.info('Connected to ' + self.alias +
            ' at ' + self.host + ':' + self.port);

          client.close = function (callback) {
            done();
            callback();
          };

          // TODO: not working yet, waiting for pg-cursor to mature
          // client.cursor = function (query, callback) {
          //   // allow query as a string if values array is not required
          //   if (typeof query === 'string') query = { text: query, values: [] };
          //   var cs = new pgCursor(query.text, query.values);
          //   var cursor = client.query(cs);
          //   // use callback function if provided
          //   if (callback) process.nextTick(callback.bind(this, null, cursor));
          //   return cursor;
          // };
        }

        return callback(err, client);
      });
      break;
    default:
      throw new Error('Invalid database type: ' + self.type);
      break;
  }
};

Database.prototype.close = function (callback) {
  if (typeof callback !== 'function') {
    callback = function () {}; // noop
  }

  if (this.connecting) { // connection in progress, try again
    return setTimeout(this.close.bind(this, callback), 100);
  } else if (this.client && this.connected) { // is connected
    return this.client.close(callback);
  } else { // not connected or connecting so just return next
    return process.nextTick(callback);
  }
};

Database.prototype.query = function (query, options, callback) {
  if (arguments.length === 2) {
    callback = options;
    options = {};
  }

  if (typeof callback !== 'function') {
    throw new Error('callback is required');
  }

  return callback('method is not yet implemented');
};

Database.prototype.queryArray = function (query, options, callback) {
  if (arguments.length === 2) {
    callback = options;
    options = {};
  }

  if (typeof callback !== 'function') {
    throw new Error('callback is required');
  }

  switch (this.type) {
    case DB_TYPE_MONGODB:
      for (var collection in query) {
        this.client.collection(collection)
          .find(query[collection], options)
          .toArray(callback);
      }
      break;
    case DB_TYPE_POSTGRESQL:
      // allow query as a string if values array is not required
      if (typeof query === 'string') query = { text: query, values: [] };

      this.client.query(query, function (err, results) {
        if (err) return callback(err);
        return callback(null, results && results.rows || null);
      });
      break;
  };
};

Database.prototype.queryStream = function (query, options, callback) {
  if (arguments.length === 2) {
    callback = options;
    options = {};
  }

  if (typeof callback !== 'function') {
    throw new Error('callback is required');
  }

  switch (this.type) {
    case DB_TYPE_MONGODB:
      for (var collection in query) {
        var cursor = this.client.collection(collection)
          .find(query[collection], options);
        var stream = cursor.stream();

        stream.on('end', cursor.close);
        process.nextTick(callback.bind(this, null, stream));
      }
      break;
    case DB_TYPE_POSTGRESQL:
      // allow query as a string if values array is not required
      if (typeof query === 'string') query = { text: query, values: [] };

      // execute query and create readable stream
      var qs = new PgQueryStream(query.text, query.values, options);
      var stream = this.client.query(qs);

      process.nextTick(callback.bind(this, null, stream));
      break;
  };
};

Database.prototype.insert = function (insert, options, callback) {
  if (arguments.length === 2) {
    callback = options;
    options = {};
  }

  if (typeof callback !== 'function') {
    throw new Error('callback is required');
  }

  switch (this.type) {
    case DB_TYPE_MONGODB:
      for (var collection in insert) {
        this.client.collection(collection)
          .insert(
            insert[collection],
            options,
            callback
          );
      }
      break;
    case DB_TYPE_POSTGRESQL:
      process.nextTick(callback.bind(this, 'not yet implemented'));
      break;
  };
};

Database.prototype.update = function (query, update, options, callback) {
  if (arguments.length === 3) {
    callback = options;
    options = {};
  }

  if (typeof callback !== 'function') {
    throw new Error('callback is required');
  }

  switch (this.type) {
    case DB_TYPE_MONGODB:
      for (var collection in query) {
        this.client.collection(collection)
          .update(
            query[collection],
            update[collection],
            options,
            callback
          );
      }
      break;
    case DB_TYPE_POSTGRESQL:
      process.nextTick(callback.bind(this, 'not yet implemented'));
      break;
  };
};

Database.prototype.upsert = function (query, update, options, callback) {
  if (arguments.length === 3) {
    callback = options;
    options = {};
  }

  if (typeof callback !== 'function') {
    throw new Error('callback is required');
  }

  options.upsert = true;

  return this.update(query, update, options, callback);
};

module.exports = Database;
