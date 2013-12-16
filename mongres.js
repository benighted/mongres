var fs = require('fs');
var path = require('path');
var async = require('async');

var Mongres = require('./index');

var configPaths = [];
var period = null;
var debug = false;

// process cli arguments
for (var i = 2; i < process.argv.length; i++) {
  switch (process.argv[i]) {
    // debugging options
    case "-d":
    case "-db": // works with nodemon
    case "--debug":
      debug = true;
      break;

    // time period between executions
    case "-p":
    case "--period":
      if (period) throw new Error('period specified more than once');
      period = parseInt(process.argv[++i], 10);
      break;

    // input config file(s) specification
    case "-f":
    case "--file":
      ++i; // use next argument for file path
    default: // assume a file path by default
      try {
        var cPath = path.resolve(process.argv[i]);
        var cIndex = configPaths.length;

        configPaths.push(cPath);
        fs.watchFile(cPath, function (prev, curr) {
          console.log('Detected change to ' + cPath);
          delete require.cache[require.resolve(cPath)];
        });
      } catch (err) {
        throw new Error('invalid module specified: ' + process.argv[i]);
      }
      break;
  }
}

(function runConfigs() {
  var start = new Date();
  // run config sets in parallel
  async.each(configPaths, function (cPath, next) {
    var cModule = require(cPath);
    if (debug) cModule.debug = debug;
    new Mongres(cModule).run(next);
  }, function (err) {
    if (err) {
      console.error(err);
      process.exit(1);
    } else if (!period) {
      process.exit(0);
    } else {
      var diff = (new Date().getTime() - start.getTime()) / 1000;
      diff = period > diff ? Math.ceil(period - diff) : 1;
      setTimeout(runConfigs, diff * 1000);
    }
  });
})();
