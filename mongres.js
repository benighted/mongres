var path = require('path');
var async = require('async');

var Mongres = require('./index');

var configs = [];
var debugMode = false;

// process cli arguments
if (process.argv) {
  for (var i = 2; i < process.argv.length; i++) {
    switch (process.argv[i]) {
      case "-d":
      case "-db": // works with nodemon
      case "--debug":
        debugMode = true;
        break;
      default:
        configs.push(require(path.resolve(process.argv[i])));
        break;
    }
  }
}

// run operation sets in parallel
async.each(configs, function (config, next) {
  if (debugMode) config.debug = debugMode;
  new Mongres(config).run(next);
}, function (err) {
  if (err) return console.error(err);
});
