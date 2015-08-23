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
			};

		_.forOwn(requiredParams, function(validator, param) {
			var value = _.get(config, param);
			if (!value) return paramMissing(param);
			validateParamValue(validator, param, value);
		});

		_.forOwn(optionalParams, function(validator, param) {
			var value = _.get(config, param);
			if (value) validateParamValue(validator, param, value);
		});

		self.config = config;
		self.dbapi = dbapi.PgQDatabaseAPI(config.source.database);
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
		if (self.logDebug) {
			self.emit('log', message);
		}
	};

	// step 1 register this consumer for the queue
	self.connect = function() {
		dbapi.registerConsumer(
			self.config.source.queue,
			self.config.consumerName,
			emitWhenDone('consumerRegistered')
		);
	};

	// just a hook in case somebody wants to hook into it externally
	self.on('consumerRegistered', function() {
		self.emit('connect');
		self.emit('pollEvents');
	});

	// actual work cycle
	self.on('pollEvents', function() {
		dbapi.loadNextBatch({
				queueName: self.config.source.queue,
				consumerName: self.config.consumerName,
				// minLag etc. is not yet implemented
			},
			emitWhenDone('batchLoaded')
		);
	});

	self.on('batchLoaded', function(batch) {
		if (batch.batch_id === null) {
			setTimeout(emitWhenDone('pollEvents'), self.pollingInterval || 1000);
		} else {
			dbapi.loadBatchEvents({
					queueName: self.config.source.queue,
					consumerName: self.config.consumerName,
					batchId: batch.batch_id
				},
				function(err, response) {
					var eb = new eventBatch(
						self,
						self.config.source.queue,
						batch.batch_id,
						response
					);
					self.emit('batchEventsLoaded', batch.batch_id, eb.events);
				}
			);
		}
	});

	self.on('batchEventsLoaded', function(batchId, batchEvents) {
		// with empty batch immediately mark it as processed
		if (batchEvents.length === 0) {
			dbapi.finishBatch(batchId, emitWhenDone('batchProcessed'));
		}
		_.each(batchEvents, function(batchEvent) {
			self.emit('event', batchEvent);
		});
	});

	self.on('batchProcessed', function() {
		self.log('batch processed and marked as completed');
		// continue work on next batch
		self.emit('pollEvents');
	});

	self.on('batchFailed', function() {
		// marking batch as processed failed, sleep a bit and continue work
		// if batch is left unmarked it will be refetched in the next cycle
		// this continues until the batch is finally marked done
		setTimeout(function() {
			self.emit('pollEvents');
		}, self.afterFailureInterval || 5000);
	});

	initialize(config);
	return self;
};
