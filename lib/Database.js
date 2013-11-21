var pg = require('pg');
require("pg-parse-float")(pg);
// var pgCursor = require('pg-cursor');
var pgQueryStream = require('pg-query-stream');

var mg = require('mongodb');

var DB_TYPE_MONGODB    = 'mongodb';
var DB_TYPE_POSTGRESQL = 'postgresql';

function Database(config) {
  if (!config.type) throw new Error('Database type is required');
  if (!config.name) throw new Error('Database name is required');
  if (!config.host) throw new Error('Database host is required');

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
  this.type = config.type;
  this.name = config.name;
  this.host = config.host;
  this.port = config.port;
  this.user = config.user;
  this.pass = config.pass;
}

Database.prototype.connect = function (callback) {
  if (this.client) {
    if (this.connecting) {
      return setTimeout(this.connect.bind(this, callback), 100);
    } else if (this.connected) {
      return process.nextTick(function () {
        callback(null, this.client);
      });
    }
  }

  var self = this;
  self.connecting = true;
  self.connected = false;

  switch (self.type) {
    case DB_TYPE_MONGODB:
      var url = "mongodb://" + self.host + ":" + self.port + "/" + self.name;

      return mg.connect(url, function(err, client) {
        self.connecting = false;

        if (client) {
          self.connected = true;
          self.client = client;
        }

        return callback(null, client);
      });
      break;
    case DB_TYPE_POSTGRESQL:
      var url = "postgres://" +
        (self.user ? self.user + (self.pass ? ":" + self.pass : "") + "@" : "") +
        (self.host + (self.port ? ":" + self.port : "") + "/" + self.name);

      return pg.connect(url, function(err, client, done) {
        self.connecting = false;

        if (client) {
          self.connected = true;
          self.client = client;

          client.close = done;

          client.stream = function (query, options, callback) {
            // options object is optional
            if (arguments.length === 2) {
              callback = options;
              options = undefined;
            }

            // allow query as a string if values array is not required
            if (typeof query === 'string') query = { text: query, values: [] };

            // execute query and create readable stream
            var qs = new pgQueryStream(query.text, query.values, options);
            var stream = client.query(qs);

            // use callback function if provided
            if (callback) process.nextTick(callback.bind(this, null, stream));

            return stream;
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
  }
};

Database.prototype.close = function (callback) {
  if (this.client && this.connected) {
    return this.client.close(callback);
  } else if (this.client && this.connecting) {
    return setTimeout(this.close.bind(this, callback), 100);
  } else {
    return process.nextTick(callback);
  }
};

module.exports = Database;
