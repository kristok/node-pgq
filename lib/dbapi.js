var db = require('./db'),
	eventBatch = require('./eventbatch'),
	_ = require('lodash');

exports.PgQDatabaseAPI = function(config, consumer) {
	var self = this;

	var initialize = function(config, consumer) {
		self.db = db(config.source.database);
		self.consumerName = config.consumerName;
		self.queueName = config.source.queue;

		// to be implemented in next iterations
		self.minLag = null;
		self.minCount = null;
		self.minInterval = null;
		// use cursor instead of fetching all data at once
		self.fetchWithCursor = false;
		self.consumer = consumer;
	};

	self.registerConsumer = function(callback) {
		self.db.query.first(
			'SELECT pgq.register_consumer($1, $2)',
			[self.queueName, self.consumerName],
			callback
		);
	};

	self.loadNextBatch = function(callback) {
		var query = [
			'SELECT * FROM pgq.next_batch_custom (',
			'    $1::text,',
			'    $2::text,',
			'    $3::interval,',
			'    $4::integer,',
			'    $5::interval',
			');'
		].join('\n');

		self.db.query.first(query, [
			self.queueName,
			self.consumerName,
			self.minLag,
			self.minCount,
			self.minInterval
		], callback);
	};

	var loadBatchEventsAtOnce = function(batchId, callback) {
		var query = 'select * from pgq.get_batch_events($1::int)',
			batch;

		self.db.query(query, [batchId], function(err, eventData) {
			// TODO : collapse if we don't need to extend events
			if (err) {
				callback(err);
			} else {
				batch = new eventBatch(self.consumer, self.queueName, batchId, eventData);
				callback(null, batchId, batch.events);
			}
		});
	};

	self.loadBatchEvents = function(batchId, callback) {
		if (self.fetchWithCursor) {
			throw new Error('not implemented');
		} else {
			loadBatchEventsAtOnce(batchId, callback);
		}
	};

	self.finishBatch = function(batchId, callback) {
		self.db.query.first('select pgq.finish_batch($1::bigint)', [batchId], callback);
	};

	self.eventTagRetrySeconds = function(batchId, eventId, delay, callback) {
		self.db.query('select pgq.event_retry($1::bigint, $2::bigint, $3::integer)', [
			batchId,
			eventId,
			delay
		], callback);
	};

	self.eventTagRetryTimestamp = function(batchId, eventId, reinsertTimestamp, callback) {
		self.db.query('select pgq.event_retry($1::bigint, $2::bigint, $3::timestamptz)', [
			batchId,
			eventId,
			reinsertTimestamp
		], callback);
	};

	initialize(config, consumer);
	return self;
};
