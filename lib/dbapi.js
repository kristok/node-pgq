var db = require('./db'),
	eventBatch = require('./eventbatch'),
	DUPLICATE_OBJECT = '42710';

exports.PgQDatabaseAPI = function(config) {
	var self = this;

	var initialize = function(dbConString) {
		self.db = db(dbConString);

		// implement: use cursor instead of fetching all data at once
		self.fetchWithCursor = false;
	};

	self.registerConsumer = function(queueName, consumerName, callback) {
		console.log('queueue = '+queueName);
		self.db.query.first(
			'SELECT pgq.register_consumer($1, $2)',
			[queueName, consumerName],
			callback
		);
	};

	self.loadNextBatch = function(params, callback) {
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
			params.queueName,
			params.consumerName,
			params.minLag,
			params.minCount,
			params.minInterval
		], callback);
	};

	var loadBatchEventsAtOnce = function(params, callback) {
		var query = 'SELECT * from pgq.get_batch_events($1::int)';
		self.db.query(query, [params.batchId], callback);
	};

	self.loadBatchEvents = function(params, callback) {
		if (self.fetchWithCursor) {
			throw new Error('not implemented');
		} else {
			loadBatchEventsAtOnce(params, callback);
		}
	};

	self.finishBatch = function(batchId, callback) {
		self.db.query.first('SELECT pgq.finish_batch($1::bigint)', [batchId], callback);
	};

	self.eventTagRetrySeconds = function(batchId, eventId, delay, callback) {
		self.db.query('SELECT pgq.event_retry($1::bigint, $2::bigint, $3::integer)', [
			batchId,
			eventId,
			delay
		], callback);
	};

	self.eventTagRetryTimestamp = function(batchId, eventId, reinsertTimestamp, callback) {
		self.db.query('SELECT pgq.event_retry($1::bigint, $2::bigint, $3::timestamptz)', [
			batchId,
			eventId,
			reinsertTimestamp
		], callback);
	};

	self.createQueue = function(queueName, callback) {
		console.log('creating queue');
		self.db.query.first('SELECT pgq.create_queue($1::text);', [
			queueName
		], callback);
	};

	var escapeTableTriggerParams = function(tableName, queueName, callback) {
		self.db.query.first('SELECT $1::regclass as table, $2::text as queue', [
			tableName, queueName
		], callback);
	};

	self.addTableTrigger = function(tableName, queueName, callback) {

		// escaping is usually done by PostgreSQL server but with create trigger
		// statement it does not seem to work - so manual escaping via query
		escapeTableTriggerParams(tableName, queueName, function(err, escaped) {
			if (err) return callback(err);

			escaped.queue = '\'' + escaped.queue + '\'';

			var query = [
				'CREATE TRIGGER logutriga',
				'AFTER INSERT OR UPDATE OR DELETE ON ' + escaped.table,
				'FOR EACH ROW',
				'EXECUTE PROCEDURE pgq.logutriga(' + escaped.queue + ');'
			].join('\n');

			self.db.query(query, [], function(err) {
				if (err && err.code !== DUPLICATE_OBJECT) {
					return callback(err);
				}
				callback(null, true);
			});

		});

	};

	initialize(config);
	return self;
};
