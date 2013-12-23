var fs = require('fs');
var path = require('path');
var async = require('async');

var Mongres = require('./index');
var pkg = require('./package.json');

var configPaths = [];
var period = null;
var debug = false;
var verbose = true;

// process cli arguments
for (var i = 2; i < process.argv.length; i++) {
  switch (process.argv[i]) {
    // help output
    case "-h":
    case "--help":
      console.log(
        'Mongres v' + pkg.version + ' by ' + pkg.author + '\n' +
        '\nUsage: node ' + pkg.main + ' [ options ] < file1, file2, ... >\n\n' +
        '  -h, --help           Display usage help\n' +
        '  -v, --version        Display version number\n' +
        '  -d, --debug          Enable debug mode\n' +
        '  -q, --quiet          Disable verbose output\n' +
        '  -p, --period         Specify periodic execution ( in sec )\n' +
        '  -f, --file           Specify module file to load ( default )\n' +
        '\nReport issues or suggestions at ' + pkg.bugs.url);
      process.exit(0);
      break;

    // version output
    case "-v":
    case "--version":
      console.log(pkg.version);
      process.exit(0);
      break;

    // debug mode
    case "-d":
    case "--debug":
      debug = true;
      break;

    // quiet mode 
    case "-q":
    case "--quiet":
      verbose = false;
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

if (verbose) console.log('Mongres v%s starting up...', pkg.version);

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
