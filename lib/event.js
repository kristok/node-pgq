var _ = require('lodash'),
	url = require('url'),
	DONE = 1,
	RETRY = 0,
	UNTAGGED = -1;

var Event = function(batchTracker, queueName, batchId, eventData) {
	this.batchTracker = batchTracker;
	this.queueName = queueName;
	this.batchId = batchId;
	this.state = UNTAGGED;

	var attributes = _.mapKeys(eventData, function(value, key) {
		return key.replace('ev_', '');
	});

	attributes.data = url.parse('?' + attributes.data, true).query;
	_.extend(this, attributes);
};

// It is actually the batch that does all the heavy lifting
// these methods are exposed in event for convenience
Event.prototype.tagDone = function(callback) {
	this.batchTracker.tagDone(this.id, callback);
};

Event.prototype.tagRetry = function(retry, callback) {
	if (_.isNumber(retry)) {
		this.batchTracker.tagRetrySeconds(this.id, retry, callback);
	} else if (_.isDate(retry) || _.isString(retry)) {
		this.batchTracker.tagRetryTimestamp(this.id, retry, callback);
	} else {
		throw new Error('retry needs seconds or date as the second argument');
	}
};
Event.prototype.tagBatchDone = function() {
	this.batchTracker.batchDone(this.batchId);
};

Event.prototype.failBatch = function() {
	this.batchTracker.failBatch();
};

module.exports = Event;
module.exports.DONE = DONE;
module.exports.RETRY = RETRY;
module.exports.UNTAGGED = UNTAGGED;
