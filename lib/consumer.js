var EventEmitter = require('events').EventEmitter,
	_ = require('lodash'),
	eventBatch = require('./eventbatch'),
	dbapi = require('./dbapi');

// pgq.Consumer()
module.exports = function(config) {
	var self = new EventEmitter();

	var initialize = function(config) {
		var requiredParams = {
				consumerName: 'isString',
				'source.database': 'isString',
				'source.queue': 'isString',
			},
			optionalParams = {
				pollingInterval: 'isNumber',
				afterFailureInterval: 'isNumber',
				logDebug: 'isBoolean'
			},
			sec = 1000;

		config = _.cloneDeep(config);

		// validate that default aprameters exist and have correct type
		_.forOwn(requiredParams, function(validator, param) {
			var value = _.get(config, param);
			if (!value) return paramMissing(param);
			validateParamValue(validator, param, value);
		});

		// validate if optional parameters have correct type
		_.forOwn(optionalParams, function(validator, param) {
			var value = _.get(config, param);
			if (value) validateParamValue(validator, param, value);
		});

		// set optional parameters to their defaults
		config.pollingInterval = config.pollingInterval || 1 * sec;
		config.afterFailureInterval = config.afterFailureInterval || 5 * sec;

		self.config = config;
		self.dbapi = new dbapi.PgQDatabaseAPI(config.source.database);
	};

	var validateParamValue = function(validator, param, value) {
		// throws error when value type does not match expectation
		var isValid = _[validator](value);
		if (!isValid) return paramWrongType(param, validator);
	};

	var paramMissing = function(paramName) {
		var errMessage = 'required config parameter: '+paramName+' missing';
		throw new Error(errMessage);
	};

	var paramWrongType = function(paramName, expectedType) {
		var errMessage = 'config parameter ' + paramName + 'has wrong type,';
		errMessage += ' expected ' + expectedType;
		throw new Error(errMessage);
	};

	var emitWhenDone = function(eventToBeEmitted) {
		var resultHandler = function() {
			// first argument is error, the rest are results
			var err = arguments[0],
				results = [].slice.apply(arguments).slice(1);

			if (err) {
				self.emit('error', err);
			} else {
				// emit event (eventToBeEmitted, result1, result2, ...)
				results.unshift(eventToBeEmitted);
				self.emit.apply(self, results);
			}
		};
		return resultHandler;
	};

	self.log = function(message) {
		if (self.config.logDebug) {
			self.emit('log', message);
		}
	};

	// step 1 register this consumer for the queue
	self.connect = function() {
		self.log('registering consumer in source database');
		self.dbapi.registerConsumer(
			self.config.source.queue,
			self.config.consumerName,
			emitWhenDone('connected')
		);
	};

	// step 2 - find if there is a new batch ready
	self.pollBatch = function() {
		self.log('polling for event batch');
		self.dbapi.loadNextBatch({
				queueName: self.config.source.queue,
				consumerName: self.config.consumerName,
				// minLag etc. is not yet implemented
			},
			emitWhenDone('batchLoaded')
		);
	};

	self.loadBatchEvents = function(batchId, callback) {
		self.dbapi.loadBatchEvents({
			queueName: self.config.source.queue,
			consumerName: self.config.consumerName,
			batchId: batchId
		}, function(err, rawBatch) {
			var batch = new eventBatch(
				self,
				self.config.source.queue,
				batchId,
				rawBatch
			);
			callback(err, batchId, batch.events);
		});
	};

	self.loadBatchEventsIfAny = function(batch) {
		var sleepSec;
		if (batch.batch_id === null) {
			sleepSec = (self.config.pollingInterval / 1000);
			self.log('no batch, sleeping for ' + sleepSec + ' seconds');
			setTimeout(emitWhenDone('pollBatch'), self.config.pollingInterval);
		} else {
			self.log('polling for batch events');
			self.loadBatchEvents(
				batch.batch_id,
				emitWhenDone('batchEventsLoaded')
			);
		}
	};

	self.on('batchEventsLoaded', function(batchId, batchEvents) {
		// with empty batch immediately mark it as processed
		if (batchEvents.length === 0) {
			self.log('empty batch');
			self.dbapi.finishBatch(batchId, emitWhenDone('batchProcessed'));
		} else {
			self.log('got ' + batchEvents.length + ' events ');
		}

		// at this point we hand the control over to the consumer implementation
		// once the consumer has marked each and every batch with
		// ev.tagDone ev.tagRetry the batch will try to update it's status
		// within the database. If it's successful it calls batchProcessed
		// when an error happens we move to the error handler below that causes
		// the consumer to sleep and retry after a while.
		_.each(batchEvents, function(batchEvent) {
			self.emit('event', batchEvent);
		});
	});

	self.on('batchProcessed', function() {
		self.log('batch processed and marked as completed');
		// continue work on next batch
		self.emit('pollBatch');
	});

	self.on('error', function(err) {
		self.log('batch failed with error ' + err.message);
		self.log('sleeping ' + self.config.afterFailureInterval);

		setTimeout(function() {
			self.emit('pollBatch');
		}, self.config.afterFailureInterval || 5000);
	});

	self.on('connected', self.pollBatch); // fist run
	self.on('pollBatch', self.pollBatch);
	self.on('batchLoaded', self.loadBatchEventsIfAny);

	initialize(config);
	return self;
};
