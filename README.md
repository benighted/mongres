# Mongres ETL System for Node.js
### Synchronize PostgreSQL and MongoDB using Node.js

Mongres uses configurable modules containing database and operation definitions to perform arbitrary ETL _(extract, transform, load)_ procedures.  This project is still in development and so core fundamentals could be changed without notice.  Contributions, comments, and issue reports are __always__ welcome.


### Installation

    npm install mongres


### Command Line Usage

    node mongres <path to module file>


### Database Definitions

Databases can be defined and used almost interchangably with very minor differences.  The database abstraction layer provides the following methods:

- __queryArray (query, [options], callback)__
  * Execute `query` using `options` if given and pass the result set as an array to `callback` function.
  * `query` format for MongoDB is `{collectionName: {field: value}}`
  * `query` format for PostgreSQL is `{text: 'select foo from bar where baz > $1 and bar < $2', values: [1,2]}`
  * `callback` signature is `function (error, array)`
  * Caution: all records will be loaded into memory.  Do not use for very large data sets.
- __queryStream (query, [options], callback)__
  * Execute `query` using `options` if given and pass a readable result stream to the `callback` function.
  * `query` format for MongoDB is `{collectionName: {field: value}}`
  * `query` format for PostgreSQL is `{text: 'select foo from bar where baz > $1 and bar < $2', values: [1,2]}`
  * `callback` signature is `function (error, stream)`
- __insert (insert, [options], callback)__
  * Inserts `insert` into database using `options` if given.
  * `insert` format is `{collectionOrTableName: {field: value}}`
  * `callback` signature is `function (error, result)` where `result` is the number of affected records.
- __update (query, update, [options], callback)__
  * Applies `update` to records matching `query` using `options` if given.
  * `query` format is `{collectionOrTableName: {field: value}}`
  * `update` format is `{collectionOrTableName: {field: value}}`
  * `callback` signature is `function (error, result)` where `result` is the number of affected records.
- __upsert (query, update, [options], callback)__
  * Applies `update` to records matching `query` using `options` if given.
  * A new record will be inserted if no matching records are found.
  * `query` format is `{collectionOrTableName: {field: value}}`
  * `update` format is `{collectionOrTableName: {field: value}}`
  * `callback` signature is `function (error, result)` where `result` is the number of affected records.


### Operation Definitions

The `db` parameter will be a `Database` instance configured for the specified database.  The `registry` parameter is a storage object that is shared among all functions for the entire operation. The operation functions are executed in this order:

- __init (db, registry, callback)__ (_optional_)
  * Executed once and must call `callback` method to proceed with operation.
  * Useful for getting delta starting points or populating initial registry values.
- __extract (db, registry, process, done)__
  * Executed once, used to extract data from source database.
  * `process` method should be called once for each record
  * `done` method should be called after all records have been processed
- __transform (db, registry, data)__ (_optional_)
  * Executed once for each record emitted by `extract` functions.
  * Used to reshape data before insertion into target database.
  * Called synchronously, so just `return` the transformed data.
- __load (db, registry, data, callback)__
  * Executed once for each record emitted by `extract` and `transform` functions.
  * Can be used to populate registry with aggregated or incremental data.
- __exit (db, registry, callback)__ (_optional_)
  * Executed once after all other functions have finished.
  * Useful for cleaning up after operation, and for recording summary data.


### Modules

A Mongres module should export a JSON object like this:
```
module.exports = {

  db: {

    mongo: {
      type: 'mongodb',
      name: 'test',
      host: 'localhost',
      port: 27017,
      user: 'username',
      pass: 'password'
    },

    postgres: {
      type: 'postgresql',
      name: 'tnt2',
      host: 'localhost',
      port: 5432,
      user: 'username',
      pass: 'password'
    }

  },

  op: {

    name: 'My Data Sync',

    init: { // init is optional
      // read data from "mongo" database into registry
      mongo: function (db, registry, cb) {
        db.queryArray(
          { // query
            sync_meta: {
              _id: 'test'
            }
          },
          { // options
            limit: 1,
            fields: {
              _id: 0,
              lastChange: 1
            }
          },
          function (err, docs) {
            if (err) return cb(err);

            if (docs && docs.length) { // docs found
              var doc = docs.shift(); // use first doc
              registry.lastChange = doc.lastChange;
            } else { // use current date by default
              registry.lastChange = new Date(0);
            }

            return cb(); // continue to "extract" step
          }
        );
      }
    },

    extract: { // extract is required
      // stream data from "postgres" database to transform and load functions
      postgres: function (db, registry, process, cb) {
        db.queryStream(
          { // query
            text: "select now(), generate_series(1,10000)",
            values: []
          }, // options parameter has been omitted
          function (err, stream) { // callback function
            if (err) return cb(err);

            var error = null, procs = 0, limit = 10;

            // call "process" method for each record
            stream.on('data', function (data) {
              procs++; // increment process counter

              // pause stream if procs over limit
              if (procs >= limit) stream.pause();

              // pass data in to "transform" and "load" steps
              process(data, function (err) {
                procs--; // decrement process counter

                if (err) {
                  error = err;
                  return stream.emit('end');
                }

                // resume stream when procs are under limit
                if (!err && procs < limit) stream.resume();
              });
            });

            // execute callback function when ended
            stream.on('end', cb.bind(this, error));
          }
        );
      }
    },

    transform: { // transform is optional
      // transform data from "postgres" database into a different format
      postgres: function (db, registry, data) {
        return { // transformed data
          _id: data.generate_series,
          date: new Date(),
          arr: [0,1,2],
          obj: {a: 'a', b: 'b'}
        };
      }
    },

    load: { // load is required
      // load data from all sources into "mongo" database
      mongo: function (db, registry, data, cb) {
        if (!data) return cb('No data was given.');

        db.upsert(
          { // query
            test: {
              _id: data._id
            }
          },
          { // update
            test: data
          },
          { // options
            w: 1,
            journal: true,
            upsert: true
          },
          function (err, result) {
            if (err) return cb(err);

            // determine most recent changed date
            if (!registry.lastChange || registry.lastChange < data.changed) {
              registry.lastChange = data.changed;
            }

            return cb(null, result); // proceed to next load functions or exit
          }
        );
      }
    },

    exit: {
      mongo: function (db, registry, cb) {
        db.upsert(
          { // query
            sync_meta: {
              _id: 'test'
            }
          },
          { // update
            sync_meta: {
              $set: {
                lastChange: registry.lastChange
              }
            }
          },
          { // options
            w: 1,
            journal: true,
            upsert: true
          },
          cb // close connections and end the operation
        );
      }
    }

  }
};

```
