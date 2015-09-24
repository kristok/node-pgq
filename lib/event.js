var _ = require('lodash'),
	DONE = 1,
	RETRY = 0,
	UNTAGGED = -1;

// currently only supports events in the new log_json format
// add backward compatibility here if needed
var Event = function(batchTracker, queueName, batchId, eventData) {
	var ev = _.clone(eventData);
	if (ev.type !== 'log_json') {
		throw new Error('received event in incorrect format, expecting type: log_json');
	}

	ev.data = JSON.parse(ev.data);
	ev.prev_data = JSON.parse(ev.prev_data);
	if (_.isString(ev.primary_keys)) {
		ev.primary_keys = ev.primary_keys.split(',');
	} else {
		ev.primary_keys = [];
	}
	ev.batchTracker = batchTracker;
	ev.queueName = queueName;
	ev.batchId = batchId;
	ev.state = UNTAGGED;

	_.extend(this, ev);
};

// It is actually the batch that does all the heavy lifting
// these methods are exposed in event for convenience
Event.prototype.tagDone = function(callback) {
	this.batchTracker.tagDone(this.id, callback);
};

Event.prototype.tagRetry = function(retry, callback) {
	var retryIsDate = _.isDate(retry) || _.isString(retry);
	if (_.isNumber(retry)) {
		this.batchTracker.tagRetrySeconds(this.id, retry, callback);
	} else if (retryIsDate) {
		this.batchTracker.tagRetryTimestamp(this.id, retry, callback);
	} else {
		throw new Error('retry needs seconds or date as the second argument');
	}
};

Event.prototype.failBatch = function() {
	this.batchTracker.failBatch();
};

module.exports = Event;
module.exports.DONE = DONE;
module.exports.RETRY = RETRY;
module.exports.UNTAGGED = UNTAGGED;
