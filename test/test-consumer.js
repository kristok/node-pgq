var sinon = require('sinon'),
	proxyrequire = require('proxyquire'),
	Event = require('../lib/event'),
	assert = require('assert');

describe('consumer.js', function() {
	var subject,
		fake,
		config,
		consumer,
		dbApiInterface;

	beforeEach(function() {
		dbApiInterface = {
			registerConsumer: null
		};
		var dbapi = {
				PgQDatabaseAPI: sinon.stub().returns(dbApiInterface)
			},
			eventbatch = sinon.spy();


		fake = {
			'./dbapi': dbapi,
			dbapi: dbapi,
			'./eventbatch': eventbatch,
			eventbatch: eventbatch
		};
		config = {
			consumerName: 'testconsumer',
			source: {
				database: 'postgres://dev:dev@localhost:5432/dev',
				queue: 'testqueue'
			}
		};
		subject = proxyrequire('../lib/consumer', fake);
	});

	// it('initializes batch correctly', function(done) {
	// 	var batch = new subject(consumer, queueName, batchId, mockData);

	// 	assert.deepEqual(batch.consumer, consumer);
	// 	assert.equal(batch.queueName, queueName);
	// 	assert.equal(batch.batchId, batchId);
	// 	assert.equal(batch.unchangedIds, mockData.length);
	// 	assert.deepEqual(batch.ids, {8: Event.UNTAGGED, 9: Event.UNTAGGED});

	// 	done();
	// });
	describe('initialization', function() {

		var validateThrow = function(subject, config, match) {
			assert.throws(
				function() {
					subject(config);
				},
				match
			);
		};

		it('succeeds when all required paremeters are given', function() {
			consumer = subject(config);
			assert.equal(consumer.config.consumerName, config.consumerName);
			assert.equal(consumer.config.source.database, config.source.database);
			assert.equal(consumer.config.source.queue, config.source.queue);
		});

		it('fails when consumerName is missing', function() {
			delete config.consumerName;
			validateThrow(subject, config, /missing/);
		});

		it('fails when source object is missing', function() {
			delete config.source;
			validateThrow(subject, config, /missing/);
		});

		it('fails when source.database is missing', function() {
			delete config.source.database;
			validateThrow(subject, config, /missing/);
		});

		it('fails when source.queue is missing', function() {
			delete config.source.queue;
			validateThrow(subject, config, /missing/);
		});

		it('fails when required parameter has incorrect type', function() {
			config.source.database = 1337;
			validateThrow(subject, config, /wrong type/);
		});

		it('fails when optional parameter has incorrect type', function() {
			config.pollingInterval = 'asdf';
			validateThrow(subject, config, /wrong type/);
		});

		it('calls dbapi with database given in config', function() {
			subject(config);
			assert.ok(fake.dbapi.PgQDatabaseAPI.calledWith(config.source.database));
		});

		it('sets default for polling interval', function() {
			consumer = subject(config);
			assert.ok(consumer.config.pollingInterval);
		});

		it('sets default for after failure interval', function() {
			consumer = subject(config);
			assert.ok(consumer.config.afterFailureInterval);
		});

	});

	it('connect calls database to register consumer and emits event', function() {
		consumer = subject(config);
		consumer.emit = sinon.spy();
		dbApiInterface.registerConsumer = sinon.stub().withArgs(
			config.source.queue,
			config.consumerName
		).yields();

		consumer.connect();

		assert.ok(consumer.emit.calledWithExactly('connected'));
	});

	it('polls events when connected', function() {
		var noError = null,
			batchId = 10;

		consumer = subject(config);
		consumer.emit = sinon.spy();
		dbApiInterface.loadNextBatch = sinon.stub().withArgs(
			config.source.queue,
			config.consumerName
		).yields(noError, batchId);

		consumer.pollBatch();
		assert.ok(consumer.emit.calledWithExactly('batchLoaded', batchId));
	});

	// it('loads batch event data when batch is not empty', function() {
	// 	var batch = {
	// 			batch_id: 10
	// 		},
	// 		noError = null;

	// 	consumer = subject(config);
	// 	consumer.emit = sinon.spy();
	// 	dbApiInterface.loadBatchEvents = sinon.stub.withArgs(
	// 		config.source.queue,
	// 		config.consumerName,
	// 		batch.batch_id
	// 	).yields(noError, [{}]);


	// });


});