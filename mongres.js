var path = require('path');
var async = require('async');

var Mongres = require('./index');

var configs = [];
var period = null;
var debugMode = false;

// process cli arguments
for (var i = 2; i < process.argv.length; i++) {
  switch (process.argv[i]) {
    case "-d":
    case "-db": // works with nodemon
    case "--debug":
      debugMode = true;
      break;
    case "-p":
    case "--period":
      if (period) throw new Error('period specified more than once');
      period = parseInt(process.argv[++i], 10);
      break;
    case "-f":
    case "--file":
      configs.push(require(path.resolve(process.argv[++i])));
      break;
    default: // assume file path by default
      configs.push(require(path.resolve(process.argv[i])));
      break;
  }
}

(function runConfigs() {
  // run config sets in parallel
  async.each(configs, function (config, next) {
    if (debugMode) config.debug = debugMode;
    new Mongres(config).run(next);
  }, function (err) {
    if (err) return console.error(err);
    if (period) setTimeout(runConfigs, period * 1000);
  });
})();
