var async = require('async'),
	files = require('./files'),
	_ = require('lodash');

var skipToEnd = function() {
	var err = new Error('skip');
	err.skip = true;
	return err;
};

module.exports = function(api, callback) {
	async.series([

		function checkIfPgQSchemaExists(next) {
			api.checkIfPgQSchemaExists(function(err, result) {
				if (err) return next(err);
				if (result && result.exists) {
					return next(skipToEnd());
				}
				next();
			});
		},

		function installPgQSchemaAndFunctions(next) {
			var sql = files.getSql();
			api.sendQuery(sql, next);
		},

	], function(err) {
		if (err && _.isError(err) && !err.skip) return callback(err);
		callback();
	});
};