var path = require('path');

var Database = require('./lib/Database');
var Operation = require('./lib/Operation');

var config = null;
var debugMode = false;

var Mongres = module.exports = function Mongres(config) {
	var op = new Operation(config);
	op.run(function (err) {
	  if (err) console.error(err);
	  op.finish(function (err) {
	    if (err) console.error(err);
	    console.log('Operation finished.');
	  });
	});
};

var configs = [];

for (var i = 2; i < process.argv.length; i++) {
  switch (process.argv[i]) {
    case "-d":
    case "--debug":
      debugMode = true;
      break;
    default:
      config = require(path.resolve(process.argv[i]));
      break;
  }
}

if (config) new Mongres(config);
