var dbapi = require('./dbapi'),
	async = require('async'),
	_ = require('lodash');


module.exports = function(dbConString) {
	var self = {};
	self.api = new dbapi.PgQDatabaseAPI(dbConString);

	self.watchTables = function(tables, queue, callback) {
		if (_.isString(tables)) tables = [tables];

		async.series([
			// TODO: installExtension?
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