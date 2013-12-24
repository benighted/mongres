# Mongres ETL System for Node.js
### Synchronize PostgreSQL and MongoDB using Node.js

Mongres uses configurable modules containing database and operation definitions to perform arbitrary ETL _(extract, transform, load)_ procedures.  This project is still in development and so core fundamentals could be changed without notice.  Contributions, comments, and issue reports are __always__ welcome.


### Installation

    npm install mongres


### Command Line Usage
```
Usage: node mongres.js [options] <file1 [file2 ...]>

  -h, --help           Display usage help
  -v, --version        Display version number
  -d, --debug          Enable debug mode
  -q, --quiet          Disable verbose output
  -p, --period         Specify periodic execution ( in sec )
  -f, --file           Specify module file to load ( default )
```

### Database Definitions

Databases can be defined and used almost interchangably with very minor differences.  The database abstraction layer provides the following methods:

- __queryArray (query, [options], callback)__
  * Execute `query` using `options` if given and pass the result set as an array to `callback` function.
  * `query` format for MongoDB is `{collectionName: {field: value}}`
  * `query` format for PostgreSQL is `{text: 'select foo from bar where foo between $1 and $2', values: [1,10]}`
  * `callback` signature is `function (error, array)`
  * Caution: all records will be loaded into memory.  Do not use for very large data sets.
- __queryStream (query, [options], callback)__
  * Execute `query` using `options` if given and pass a readable result stream to the `callback` function.
  * `query` format for MongoDB is `{collectionName: {field: value}}`
  * `query` format for PostgreSQL is `{text: 'select foo from bar where foo between $1 and $2', values: [1,10]}`
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
  __interval (db, registry, callback)__ (_optional_)
  * Executed at regular intervals after load function
  * Useful for recording incremental progress in case of failures.
- __exit (db, registry, callback)__ (_optional_)
  * Executed once after all other functions have finished.
  * Useful for cleaning up after operation, and for recording summary data.


### Modules

A Mongres module must export a JSON object like this:
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

    name: 'Sample Operation',

    init: { // init is optional
      // read data from "mongo" database into registry
      mongo: [
        function (db, registry, cb) {
          // truncate the collection
          db.remove(
            { // query
              test: {} // matches all documents
            },
            cb // callback
          );
        },

        function (db, registry, cb) {
          db.queryArray(
            { // query
              mongres: {
                _id: "test"
              }
            },
            { // options
              limit: 1,
              fields: {
                _id: 0,
                lastDate: 1
              }
            },
            function (err, docs) { // callback
              if (err) return cb(err);

              var doc = docs && docs.shift() || {};
              registry.lastDate = doc.lastDate || new Date(0);

              return cb(); // continue to "extract" step
            }
          );
        }
      ]
    },

    extract: { // extract is required
      // stream data from "postgres" database to load function(s)
      postgres: function (db, registry, load, cb) {
        db.queryStream(
          { // query
            text: "                                                            \
              SELECT                                                           \
                GENERATE_SERIES($1::int, $2::int) AS series, $3::timestamp +   \
                (RANDOM()::NUMERIC(3,2) || ' days')::INTERVAL AS date,         \
                ARRAY['a','b','c'] AS arr,'{\"a\":{\"b\":\"c\"}}'::json AS obj \
              ORDER BY date                                                    \
            ",
            values: [1, 1000, registry.lastDate]
          },
          // options parameter is not required
          function (err, stream) { // callback
            if (err) return cb(err);

            var error = null, procs = 0, limit = 10;

            // call "load" method for each record
            stream.on('data', function (data) {
              procs++; // increment process counter

              // pause stream if procs over limit
              if (procs >= limit) stream.pause();

              load(data, function (err) {
                procs--; // decrement process counter

                if (err) { // pass the error
                  stream.emit('error', err);
                  return stream.emit('end');
                }

                // resume stream when procs are under limit
                if (!err && procs < limit) stream.resume();
              });
            });

            // record errors for 'end' event
            stream.on('error', function (err) {
              error = err;
            });

            // execute callback function when ended
            return stream.on('end', cb.bind(this, error));
          }
        );
      }
    },

    transform: { // transform is optional
      // transform data from "postgres" database into a different format
      postgres: function (db, registry, data) {
        return { // transformed data
          _id: data.series,
          date: data.date,
          arr: data.arr,
          obj: data.obj
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
            journal: true
          },
          function (err, result) { // callback
            if (err) return cb(err);

            // determine most recent changed date
            if (!registry.lastDate || registry.lastDate < data.date) {
              registry.lastDate = data.date;
            }

            // proceed to next load function(s) or exit
            return cb(null, result);
          }
        );
      }
    },

    interval: { // intervals are optional
      100: { // execute every 100 records
        // write progress data to "mongo" database
        mongo: function (db, registry, cb) {
          db.upsert(
            { // query
              mongres: {
                _id: 'test'
              }
            },
            { // update
              mongres: {
                $set: {
                  lastDate: registry.lastDate
                }
              }
            },
            { // options
              w: 1,
              journal: true
            },
            cb // callback
          );
        }
      }
    },

    exit: { // exit is optional
      // write result data to "mongo" database
      mongo: function (db, registry, cb) {
        db.upsert(
          { // query
            mongres: {
              _id: 'test'
            }
          },
          { // update
            mongres: {
              $set: {
                lastDate: registry.lastDate
              }
            }
          },
          { // options
            w: 1,
            journal: true
          },
          cb // callback
        );
      }
    }

  }

};

```

### Contributing
Contributions, comments, and issue reports are __always__ welcome.  Send pull requests to the `master` branch with your proposed changes or create issues to report bugs.

### Testing
Copy `tests\db.sample.js` to `tests\db.js` and modify the credentials to match your development environment.  You will need one MongoDB and one PostgreSQL database set up and running to complete the tests.  To start tests, run `npm test` in your Mongres directory.
