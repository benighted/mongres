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
        '\nUsage: node ' + pkg.main + ' [options] <file1 [file2 ...]>\n\n' +
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
} else {
  var start = new Date(), drained = false;
  var queue = async.queue(function (mongres, next) {
    mongres.run(next);
  });

  var run = function run(cPath) {
    if (Array.isArray(cPath)) return cPath.map(run);

    fs.stat(cPath, function (err, stat) {
      if (err) return console.error(err);

      if (stat.isDirectory()) {
        return fs.readdir(cPath, function (err, cPaths) {
          if (err) return console.error(err);
          return cPaths.forEach(function (p) {
            if (p.charAt(0) === '.') return;
            run(path.join(cPath, p));
          });
        });
      } else if (stat.isFile()) {
        var cModule = require(cPath);
        if (debug) cModule.debug = debug;
        if (verbose) cModule.verbose = verbose;
        if (cModule.debug) cModule.verbose = true;
        if (cModule.verbose) console.log('Loading module: ' + cPath);
        queue.push(new Mongres(cModule), function (err) {
          if (err) console.error(err);
          if (verbose) console.log('Finished with module: ' + cPath);
        });
      }
    });
  };

  queue.drain = function () {
    if (drained) return;
    else drained = true;

    if (!period) {
      if (verbose) console.log('Finished, shutting down...');
      // kill if not closed within 10 seconds
      setTimeout(process.exit, 10000).unref();
    } else { // calculate delay sufficient to maintain the period
      var delay = (new Date().getTime() - start.getTime()) / 1000;
      delay = Math.max(Math.ceil(period - delay), 1); // min 1 second
      if (verbose) console.log('Sleeping for ' + delay + ' seconds...');
      setTimeout(function () {
        start = new Date();
        drained = false;
        run(configPaths);
      }, delay * 1000);
    }
  };

  run(configPaths);
}
