var fs = require('fs');
var path = require('path');
var async = require('async');

var Mongres = require('./index');

var configPaths = [];
var period = null;
var debug = false;
var verbose = false;

// process cli arguments
for (var i = 2; i < process.argv.length; i++) {
  switch (process.argv[i]) {
    // debugging options
    case "-d":
    case "--debug":
      debug = true;
      verbose = true;
      break;

    // verbose output
    case "-v":
    case "--verbose":
      verbose = true;
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
          if (verbose) console.log('Detected modification of ' + cPath);
          delete require.cache[require.resolve(cPath)];
        });
      } catch (err) {
        throw new Error('invalid module specified: ' + process.argv[i]);
      }
      break;
  }
}

if (!configPaths) {
  if (verbose) console.log('Nothing to do, shutting down...');
} else (function runConfigs() {
  var start = new Date();
  // run config sets in parallel
  async.each(configPaths, function (cPath, next) {
    var cModule = require(cPath);
    if (debug) cModule.debug = debug;
    if (cModule.debug) verbose = cModule.debug;
    if (verbose) cModule.verbose = verbose;
    if (cModule.verbose) console.log('Running module: ' + cPath);
    new Mongres(cModule).run(next);
  }, function (err) {
    if (err || !period) {
      if (err) console.error(err);
      if (verbose) console.log('Finished, shutting down...');
      process.exit(err ? 1 : 0);
    } else { // calculate delay sufficient to maintain the period
      var delay = (new Date().getTime() - start.getTime()) / 1000;
      delay = Math.max(Math.ceil(period - delay), 10); // min 10 seconds
      if (verbose) console.log('Sleeping for ' + delay + ' seconds...');
      setTimeout(runConfigs, delay * 1000);
    }
  });
})();
