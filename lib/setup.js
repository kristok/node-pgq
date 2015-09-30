var dbapi = require('./dbapi'),
	async = require('async'),
	EventEmitter = require('events').EventEmitter,
	installPgQ = require('./installpgq'),
	_ = require('lodash');

module.exports = function(dbConString) {
	var self = new EventEmitter();
	self.api = dbapi.PgQDatabaseAPI(dbConString);

	self.watchTables = function(tables, queue, callback) {
		self.emit('log', 'setting up watched tables');
		if (_.isString(tables)) tables = [tables];

		async.series([

			function installPgQIfNeeded(next) {
				installPgQ(self.api, self, next);
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