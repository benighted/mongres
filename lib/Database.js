var pg = require('pg');
var QueryStream = require('pg-query-stream');
var JSONStream = require('JSONStream');

var mg = require('mongodb');

var DB_TYPE_MONGODB    = 'mongodb';
var DB_TYPE_POSTGRESQL = 'postgresql';

function Database(config) {
  if (!config.type) throw new Error('Database type is required');
  if (!config.name) throw new Error('Database name is required');
  if (!config.host) throw new Error('Database host is required');

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

console.log('construct ' + config.type + '.' + config.name + ' client');
  this.config = config;
  this.type = config.type;
  this.name = config.name;
  this.host = config.host;
  this.port = config.port;
  this.user = config.user;
  this.pass = config.pass;
}

Database.prototype.connect = function (callback) {
  // if already connected, call back with reference
  if (this.client && !this.client.connecting) {
    return process.nextTick(function () {
      callback(null, this.client);
    });
  }

  // if currently connecting, try again in a few ms
  if (this.client && this.client.connecting) {
    return setTimeout(this.connect.bind(this, callback), 100);
  }

  var self = this;
  self.connecting = true;

  switch (self.type) {
    case DB_TYPE_MONGODB:
      var url = "mongodb://" + self.host + ":" + self.port + "/" + self.name;
console.log('connect ' + this.type + '.' + this.name + ' client');

      return mg.connect(url, function(err, client) {
        self.connecting = false;
        self.client = client;
        return callback(null, client);
      });
      break;
    case DB_TYPE_POSTGRESQL:
      var url = "postgres://" +
        (self.user ? self.user + (self.pass ? ":" + self.pass : "") + "@" : "") +
        (self.host + (self.port ? ":" + self.port : "") + "/" + self.name);
console.log('connect ' + this.type + '.' + this.name + ' client');

      return pg.connect(url, function(err, client, done) {
        self.connecting = false;
        self.client = client;

        if (client) { // modify the client to fit our needs
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
            var qs = new QueryStream(query.text, query.values, options);
            var stream = client.query(qs);

            // use callback function if provided
            if (callback) process.nextTick(callback.bind(this, null, stream));

            return stream;
          };
        }

        return callback(err, client);
      });
      break;
  }
};

Database.prototype.close = function (callback) {
  if (this.client) {
console.log('close ' + this.type + '.' + this.name + ' client');
    return this.client.close(callback);
  } else {
    return process.nextTick(callback);
  }
};

module.exports = Database;
