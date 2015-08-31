var EventEmitter = require('events').EventEmitter,
	async = require('async'),
	dbapi = require('./dbapi');

var Ticker = function(config) {
	var self = new EventEmitter(),
		sec = 100;

	var initialize = function(config) {
		if (config && config.database) {
			self.tickerPeriod = config.tickerPeriod || 1 * sec;
			self.maintenancePeriod = config.maintenancePeriod || 120 * sec;
			self.retryPeriod = config.retryPeriod || 30 * sec;
			self.logDebug = config.logDebug || false;

			self.dbapi = dbapi.PgQDatabaseAPI(config.database);
		} else {
			throw new Error('target database must be given');
		}
	};

	var log = function(msg) {
		if (!self.logDebug) return;
		self.emit('log', msg);
	};

	self.tick = function(callback) {
		self.dbapi.createTick(callback);
	};

	var tickerLoop = function() {
		self.tick(function(err) {
			self.ticker = setTimeout(tickerLoop, self.tickerPeriod);
			if (err) {
				self.emit('error', err);
			} else {
				log('tick done');
			}
		});
	};

	self.maintenance = function(callback) {
		self.dbapi.getMaintenanceOperations(function(err, operations) {
			if (err) return callback(err);

			async.each(operations, function(operation, next) {
				self.dbapi.maintenance(
					operation.func_name,
					operation.func_arg,
					next
				);
			}, function(err) {
				callback(err, operations);
			});
		});
	};

	var maintenanceLoop = function() {
		var opCount;
		self.maintenance(function(err, ops) {
			self.maintenanceTicker = setTimeout(
				maintenanceLoop,
				self.maintenancePeriod
			);
			if (err) {
				self.emit('error', err);
			} else {
				opCount = ops && ops.length || 0;
				log('maintenance done: ' + opCount + ' commands');
			}
		});
	};

	self.retry = function(callback) {
		self.dbapi.reinsertRetryEvents(callback);
	};

	var retryLoop = function() {
		self.retry(function(err) {
			self.retryTicker = setTimeout(
				retryLoop,
				self.retryPeriod
			);
			if (err) {
				self.emit('error', err);
			} else {
				log('retry reinsert done');
			}
		});
	};

	self.start = function() {
		tickerLoop();
		maintenanceLoop();
		retryLoop();
		log('ticker started');
	};

	self.stop = function() {
		clearTimeout(self.ticker);
		clearTimeout(self.retryTicker);
		clearTimeout(self.maintenanceTicker);
	};

	initialize(config);
	return self;
};

module.exports = function(config) {
	return new Ticker(config);
};