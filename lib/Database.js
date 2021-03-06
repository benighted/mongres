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
  this.verbose = config.verbose;

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
  if (self.verbose) console.info('Connecting to %s at %s:%d',
      self.alias, self.host, self.port);

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
          if (self.verbose) console.info('Connected to %s at %s:%d',
              self.alias, self.host, self.port);

          var done = function (err, result) {
            if (err || !result) {
              if (!err) err = 'Authentication failed.';
              return self.close(callback.bind(self, err));
            }
            if (self.verbose) console.info('Authenticated as %s.', self.user);
            return callback(err, db);
          };

          if (self.user) { // perform auth if user given
            if (self.verbose) console.info('Authenticating as %s.', self.user);
            db.authenticate(self.user, self.pass, done);
          } else {
            done(undefined, true);
          }
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
          if (self.verbose) console.info('Connected to %s at %s:%d',
              self.alias, self.host, self.port);

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
        callback(err, results && results.rows || null);
      });
      break;
  };
};

Database.prototype.queryStream = function (query, options, callback) {
  if (arguments.length === 2) {
    callback = options;
    options = {};
  }

  var retval = null;

  switch (this.type) {
    case DB_TYPE_MONGODB:
      for (var collection in query) {
        var cursor = this.client.collection(collection)
          .find(query[collection], options);
        var stream = retval = cursor.stream();

        stream.once('end', cursor.close);
        if (callback) stream.once('readable', callback.bind(this, null, stream));
      }

      break;
    case DB_TYPE_POSTGRESQL:
      // allow query as a string if values array is not required
      if (typeof query === 'string') query = { text: query, values: [] };

      var stream = retval = this.client.query(
        new PgQueryStream(query.text, query.values, options)
      );

      if (callback) stream.once('readable', callback.bind(this, null, stream));

      break;
  }

  return retval;
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

Database.prototype.save = function (update, options, callback) {
  if (arguments.length === 2) {
    callback = options;
    options = {};
  }

  if (typeof callback !== 'function') {
    throw new Error('callback is required');
  }

  switch (this.type) {
    case DB_TYPE_MONGODB:
      for (var collection in update) {
        this.client.collection(collection)
          .save(
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

Database.prototype.remove = function (query, options, callback) {
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
          .remove(
            query[collection],
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

module.exports = Database;
