var EventEmitter = require('events').EventEmitter,
	async = require('async'),
	dbapi = require('./dbapi');

// pgq.Ticker()
module.exports = function(config) {
	var self = new EventEmitter(),
		sec = 100;

	var initialize = function(config) {
		if ((!config && config.database)) {
			throw new Error('target.database must be given');
		}
		self.tickerPeriod = config.tickerPeriod || 1 * sec;
		self.maintenancePeriod = config.maintenancePeriod || 120 * sec;
		self.retryPeriod = config.retryPeriod || 30 * sec;

		self.dbapi = dbapi.PgQDatabaseAPI(config.database);
	};

	self.tick = function(callback) {
		dbapi.createTick(callback);
	};

	var tickerLoop = function() {
		self.tick(function() {
			self.ticker = setTimeout(tickerLoop, self.tickerPeriod);
		});
	};

	self.maintenance = function(callback) {
		dbapi.getMaintenanceOperations(function(err, operations) {
			if (err) return callback(err);

			async.eachSeries(operations, function(operation, next) {
				dbapi.maintenance(
					operation.func_name,
					operation.func_arg,
					next
				);
			}, callback);
		});
	};

	var maintenanceLoop = function() {
		self.maintenance(function(err) {
			self.maintenanceTicker = setTimeout(
				maintenanceLoop,
				self.maintenancePeriod
			);
			if (err) {
				console.log('error during maintenance ', err);
			}
		});
	};

	self.retry = function(callback) {
		dbapi.reinsertRetryEvents(callback);
	};

	var retryLoop = function() {
		self.retry(function(err) {
			self.retryTicker = setTimeout(
				retryLoop,
				self.retryPeriod
			);
			if (err) {
				console.log('error during retry tick ', err);
			}
		});
	};

	self.start = function() {
		tickerLoop();
		maintenanceLoop();
		retryLoop();
	};

	initialize(config);
	return self;
};