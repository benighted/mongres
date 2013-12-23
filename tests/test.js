var config = module.exports = {
  debug: 1,
  verbose: 1,

  db: require('./db.js'),

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
                GENERATE_SERIES($1::int, $2::int) AS series,                   \
                $3::timestamp + (RANDOM() || ' days')::INTERVAL AS date,       \
                ARRAY['zero','one','two'] AS arr,                              \
                '{\"a\": \"b\", \"c\": \"d\"}'::json AS obj                    \
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
