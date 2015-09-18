var dbapi = require('./dbapi'),
	async = require('async'),
	installPgQ = require('./installpgq'),
	_ = require('lodash');

module.exports = function(dbConString) {
	var self = {};
	self.api = dbapi.PgQDatabaseAPI(dbConString);

	self.watchTables = function(tables, queue, callback) {
		if (_.isString(tables)) tables = [tables];

		async.series([

			function installPgQIfNeeded(next) {
				installPgQ(self.api, next);
			},

			function createQueue(next) {
				self.api.createQueue(queue, next);
			},
			function addTableTriggers(next) {
				async.eachSeries(tables, function(table, done) {
					self.api.addTableTrigger(table, queue, done);
				}, next);
			}
		], callback);
	};

	return self;
};