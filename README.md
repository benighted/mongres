# Mongres ETL System for Node.js
### Synchronize PostgreSQL and MongoDB using Node.js


.
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
      // transform data from "postgres" database into a standardized format
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
        // todo
      }
    },

    exit: { // exit is optional
      // execute finalization commands on "mongo" database
      mongo: function (db, registry, cb) {
        // todo
      }
    }
  }
};

```
