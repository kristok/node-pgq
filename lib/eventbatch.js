var _ = require('lodash'),
	Event = require('./event');

var EventBatch = function(consumer) {
	this.consumer = consumer;
	this.queueName = consumer.config.source.queue;
	this.ids = {};
	this.events = [];
	this.dbapi = consumer.dbapi;
	this.log = consumer.log;

	return this;
};

EventBatch.prototype.allEventsProcessed = function(callback) {
	var self = this,
		newErr;


	this.dbapi.finishBatch(this.batchId, function(err, data) {
		if (err) {
			self.consumer.emit('error', err);
		} else if (!_.has(data, 'finish_batch')) {
			err = new Error('database did not respond with finish_batch');
			self.consumer.emit('error', newErr);
		} else {
			self.consumer.emit('batchProcessed');
		}
		if (callback && _.isFunction(callback)) callback(err);
	});
};

EventBatch.prototype.markEventProcessed = function(eventId, state, callback) {
	this.ids[eventId] = Event[state];
	this.unchangedIds--;
	if (this.unchangedIds === 0) {
		this.allEventsProcessed(callback);
	} else {
		if (callback && _.isFunction(callback)) callback();
	}
};

EventBatch.prototype.tagDone = function(eventId, callback) {
	this.markEventProcessed(eventId, Event.DONE, callback);
};

EventBatch.prototype.tagRetrySeconds = function(eventId, delay, callback) {
	var self = this;
	this.tagRetry(
		self.dbapi.eventTagRetrySeconds,
		eventId,
		delay,
		callback
	);
};

EventBatch.prototype.failBatch = function() {
	var err = new Error('failing batch due to event.failBatch');
	this.consumer.emit('error', err);
};

EventBatch.prototype.tagRetryTimestamp = function(eventId, reinsertTimestamp, callback) {
	var self = this;
	this.tagRetry(
		self.dbapi.eventTagRetryTimestamp,
		eventId,
		reinsertTimestamp,
		callback
	);
};

EventBatch.prototype.tagRetry = function(APIMethod, eventId, delay, callback) {
	var self = this;

	var callOrEmit = function(callback, err) {
		if (callback && _.isFunction(callback)) {
			callback(err);
		} else {
			self.consumer.emit('error', err);
		}
	};

	APIMethod(this.batchId, eventId, delay, function(err) {
		if (err) {
			callOrEmit(callback, err);
		} else {
			self.markEventProcessed(eventId, Event.RETRY, function(err) {
				callOrEmit(callback, err);
			});
		}
	});
};

EventBatch.prototype.load = function(batchId, callback) {
	var self = this;

	self.batchId = batchId;
	self.dbapi.loadBatchEvents({
		queueName: self.queueName,
		consumerName: self.consumerName,
		batchId: batchId
	}, function(err, rawBatch) {
		if (err) return callback(err);

		self.unchangedIds = rawBatch.length;

		_.each(rawBatch, function(eventDataRow) {
			var builtEvent = new Event(
				self,
				self.queueName,
				self.batchId,
				eventDataRow
			);
			self.ids[builtEvent.id] = builtEvent.state;
			self.events.push(builtEvent);
		});

		callback(null, batchId, self.events);
	});
};

module.exports = function(consumer) {
	return new EventBatch(consumer);
};
