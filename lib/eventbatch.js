var _ = require('lodash'),
	Event = require('./event');

var EventBatch = function(consumer, queueName, batchId, eventsData) {
	var self = this;

	//lookups object, it's a bit faster with huge batches than arrays
	this.ids = {};
	this.unchangedIds = eventsData.length;
	this.batchId = batchId;
	this.queueName = queueName;
	this.consumer = consumer;
	this.events = [];
	this.dbapi = consumer.dbapi;

	_.each(eventsData, function(eventDataRow) {
		var builtEvent = new Event(self, queueName, batchId, eventDataRow);
		self.ids[builtEvent.id] = builtEvent.state;
		self.events.push(builtEvent);
	});

	return this;
};

EventBatch.prototype.allEventsProcessed = function(callback) {
	var self = this;

	this.dbapi.finishBatch(this.batchId, function(err, data) {
		if (err) {
			self.consumer.emit('error', err);
		} else if (!_.has(data, 'finish_batch')) {
			err = new Error('database did not respond with finish_batch');
			self.consumer.emit(err);
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

module.exports = EventBatch;
