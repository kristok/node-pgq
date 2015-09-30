var async = require('async'),
	files = require('./files'),
	_ = require('lodash');

var skipToEnd = function() {
	var err = new Error('skip');
	err.skip = true;
	return err;
};

module.exports = function(api, setup, callback) {
	async.series([

		function checkIfPgQSchemaExists(next) {
			setup.emit('log', 'checking if pgq schema already exists');
			api.checkIfPgQSchemaExists(function(err, result) {
				if (err) return next(err);
				if (result && result.exists) {
					setup.emit('log', 'pgq schema exists');
					return next(skipToEnd());
				}
				next();
			});
		},

		function installPgQSchemaAndFunctions(next) {
			setup.emit('log', 'installing pgq schema');
			var sql = files.getSql();

			setup.emit('log', 'loaded files from disk');
			api.sendQuery(sql, function(err) {
				if (err) return next(err);
				setup.emit('log', 'pgq schema installed');
				next();
			});
		},

	], function(err) {
		if (err && _.isError(err) && !err.skip) return callback(err);
		callback();
	});
};