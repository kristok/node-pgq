var sinon = require('sinon'),
	proxyrequire = require('proxyquire'),
	assert = require('assert');

describe('consumer.js', function() {
	var subject,
		fake,
		config,
		consumer,
		dbApiInterface,
		eventBatch,
		noError = null;

	beforeEach(function() {
		dbApiInterface = {
			registerConsumer: null
		};
		eventBatch = {};
		var dbapi = {
				PgQDatabaseAPI: sinon.stub().returns(dbApiInterface)
			},
			eventBatchMock = sinon.stub().returns(eventBatch);

		fake = {
			'./dbapi': dbapi,
			dbapi: dbapi,
			'./eventbatch': eventBatchMock,
			eventbatch: eventBatchMock
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

	describe('work loop', function() {
		var clock;

		beforeEach(function() {
			consumer = subject(config);
			consumer.emit = sinon.spy();
			clock = sinon.useFakeTimers();
		});

		afterEach(function() {
			clock.restore();
		});

		describe('connect', function() {
			it('calls database to register consumer and emits event', function() {
				dbApiInterface.registerConsumer = sinon.stub().withArgs(
					config.source.queue,
					config.consumerName
				).yields();

				consumer.connect();
				assert.ok(consumer.emit.calledWithExactly('connected'));
			});
		});

		describe('pollBatch', function() {
			it('polls events when connected', function() {
				var batchId = 10;

				dbApiInterface.loadNextBatch = sinon.stub().withArgs(
					config.source.queue,
					config.consumerName
				).yields(noError, batchId);

				consumer.pollBatch();
				assert.ok(consumer.emit.calledWithExactly('batchLoaded', batchId));
			});
		});

		describe('loadBatchEventsIfAny', function() {
			it('loads batch event data when batch is not empty', function() {
				var batch = {
						batch_id: 10,
					};

				eventBatch.events = 'events';
				eventBatch.load = sinon.stub().yields(noError, batch.batch_id);

				consumer.loadBatchEventsIfAny(batch);
				assert.ok(consumer.emit.calledWithExactly(
					'batchEventsLoaded',
					batch.batch_id
				));
			});

			it('sleeps when batch is empty', function() {
				var batch = {
					batch_id: null
				};

				consumer.loadBatchEventsIfAny(batch);
				clock.tick(1000);
				assert.ok(consumer.emit.calledWithExactly('pollBatch'));
			});
		});

		describe('emitEventsToHandler', function() {
			var batchId = 10;
			it('calls finishBatch when empty batch', function() {
				var events = [];
				dbApiInterface.finishBatch = sinon.stub().yields();

				consumer.emitEventsToHandler(batchId, events);
				assert.ok(consumer.emit.calledWithExactly('batchProcessed'));
			});

			it('emits each event to handler', function() {
				var events = [1,2,3];
				consumer.emitEventsToHandler(batchId, events);
				consumer.emit.calledWith('event', 1);
				consumer.emit.calledWith('event', 2);
				consumer.emit.calledWith('event', 3);
			});
		});

		describe('batchProcessed', function() {
			it('calls pollBatch', function() {
				consumer.batchProcessed();
				assert.ok(consumer.emit.calledWithExactly('pollBatch'));
			});
		});

		describe('log messages', function() {
			it('calls pollBatch', function() {
				consumer.config.logDebug = true;
				consumer.batchProcessed();
				assert.ok(consumer.emit.calledWithExactly('pollBatch'));
			});
		});

		describe('error handling', function() {
			var expectedErr = new Error('err');

			it('emits errors', function() {
				var batchId = 10,
					events = [];

				dbApiInterface.finishBatch = sinon.stub().yields(expectedErr);
				consumer.emitEventsToHandler(batchId, events);
				assert.ok(consumer.emit.calledWith('error', expectedErr));
			});

			it('restarts work loop from beginning on error', function() {
				consumer.handleError(expectedErr);

				clock.tick(5000);
				assert.ok(consumer.emit.calledWith('pollBatch'));
			});
		});

	});
	// event loop - end
});