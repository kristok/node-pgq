var _ = require('lodash'),
	Event = require('./event'),
	dbapi = require('./dbapi');

var EventBatch = function(consumer, queueName, batchId, eventsData) {
	var self = this;

	//lookups object, it's a bit faster with huge batches than arrays
	this.ids = {};
	this.unchangedIds = eventsData.length;
	this.batchId = batchId;
	this.queueName = queueName;
	this.consumer = consumer;
	this.events = [];

	_.each(eventsData, function(eventDataRow) {
		var builtEvent = new Event(self, queueName, batchId, eventDataRow);
		self.ids[builtEvent.id] = builtEvent.state;
		self.events.push(builtEvent);
	});

	return this;
};

EventBatch.prototype.allEventsProcessed = function(callback) {
	var self = this;

	dbapi.finishBatch(this.batchId, function(err, data) {
		if (err || !_.has(data, 'finish_batch')) {
			// marking batch as processed failed
			// the error causes the consumer to sleep a bit and continue work
			// if batch is left unmarked it will be refetched in the next cycle
			// this continues until the batch is finally marked done
			self.consumer.emit('error', err);
			// the main use case for this callback is manually controlling if event
			// tagging was successful in the consumer code
			if (callback && _.isFunction(callback)) {
				callback(err);
			}
		} else {
			self.consumer.emit('batchProcessed');
			if (callback && _.isFunction(callback)) {
				callback(null, data);
			}
		}
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
	this.tagRetry(
		dbapi.eventTagRetrySeconds,
		eventId,
		delay,
		callback
	);
};

Event.prototype.tagRetryTimestamp = function(eventId, reinsertTimestamp, callback) {
	this.tagRetry(
		dbapi.eventTagRetryTimestamp,
		eventId,
		reinsertTimestamp,
		callback
	);
};

EventBatch.prototype.tagRetry = function(APIMethod, eventId, delay, callback) {
	var self = this;

	APIMethod(this.batchId, eventId, delay, function(err) {
		if (err) {
			if (callback && _.isFunction(callback)) {
				callback(err);
			} else {
				self.consumer.emit('error', err);
			}
			return;
		}

		self.markEventProcessed(eventId, Event.RETRY, callback);
	});
};

module.exports = EventBatch;
