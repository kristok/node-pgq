var sinon = require('sinon'),
	proxyrequire = require('proxyquire'),
	assert = require('assert'),
	_ = require('lodash');

describe('dbapi.js', function() {
	var subject,
		fake,
		db,
		dbModule;

	beforeEach(function() {
		db = {
			query: {}
		};
		dbModule = sinon.stub().returns(db);
		fake = {
			'./db': dbModule,
			db: dbModule
		};
		subject = proxyrequire('../lib/dbapi.js', fake);
	});

	it('initializes correctly', function() {
		var instance1 = subject.PgQDatabaseAPI('a'),
			instance2 = subject.PgQDatabaseAPI('a');

		assert.notEqual(instance1, instance2);
		assert.ok(dbModule.calledTwice);
		assert.ok(dbModule.alwaysCalledWith('a'));
	});

	describe('simple apis map params and return results', function() {
		var instance;

		beforeEach(function() {
			instance = subject.PgQDatabaseAPI('a');
		});

		var test = function(method, args, match, dbStub, done) {
			var stub,
				expectedErr = new Error('err'),
				expectedData = 'somedata',
				resultMock = sinon.stub().yields(expectedErr, expectedData),
				originalArgs = _.cloneDeep(args);

			if (!match) {
				match = originalArgs;
			}

			if (dbStub === 'first') {
				stub = db.query.first = resultMock;
			} else {
				stub = db.query = resultMock;
			}

			args.push(function(err, data) {
				assert.equal(err, expectedErr);
				assert.equal(data, expectedData);
				assert.ok(stub.calledWithExactly(
					sinon.match.string,
					match,
					sinon.match.func
				));
				done();
			});

			method.apply(this, args);
		};

		it('registerConsumer', function(done) {
			test(
				instance.registerConsumer, // method
				['queueName','consumerName'], // params
				null, // match in = out
				'first', // mocked db function
				done
			);
		});

		it('loadNextBatch', function(done) {
			var params = {
				queueName: 'queueName',
				consumerName: 'consumerName',
				minLag: 'minLag',
				minCount: 'minCount',
				minInterval: 'minInterval'
			};
			test(
				instance.loadNextBatch,
				[params],
				[
					params.queueName,
					params.consumerName,
					params.minLag,
					params.minCount,
					params.minInterval
				],
				'first',
				done
			);
		});

		it('finishBatch', function(done) {
			var batchId = 1;
			test(
				instance.finishBatch,
				[batchId],
				null, // in = out
				'first',
				done
			);
		});

		it('eventTagRetrySeconds', function(done) {
			var batchId = 1,
				eventId = 2,
				delay = 10;

			test(
				instance.eventTagRetrySeconds,
				[batchId, eventId, delay],
				null,
				'query',
				done
			);
		});

		it('eventTagRetryTimestamp', function(done) {
			var batchId = 1,
				eventId = 2,
				reinsertTimestamp = new Date();

			test(
				instance.eventTagRetryTimestamp,
				[batchId, eventId, reinsertTimestamp],
				null,
				'query',
				done
			);
		});

		it('createQueue', function(done) {
			var queueName = 'queue';

			test(
				instance.createQueue,
				[queueName],
				null,
				'first',
				done
			);
		});

		it('createTick', function(done) {
			test(
				instance.createTick,
				[],
				null,
				'first',
				done
			);
		});

		it('getMaintenanceOperations', function(done) {
			test(
				instance.getMaintenanceOperations,
				[],
				null,
				'query',
				done
			);
		});

		it('maintenance (without arg)', function(done) {
			test(
				instance.maintenance,
				['func', null],
				[],
				'first',
				done
			);
		});

		it('maintenance (with arg)', function(done) {
			test(
				instance.maintenance,
				['func', 'arg'],
				[],
				'first',
				done
			);
		});

		it('reinsertRetryEvents', function(done) {
			test(
				instance.reinsertRetryEvents,
				[],
				null,
				'first',
				done
			);
		});

		// fetching with cursor is not yet implemented
		// until that time we have only the simple test
		it('loadBatchEvents', function(done) {
			var params = {
				batchId: 1
			};
			test(
				instance.loadBatchEvents,
				[params],
				[params.batchId],
				'query',
				done
			);
		});

		it('checkIfPgQSchemaExists', function(done) {
			test(
				instance.checkIfPgQSchemaExists,
				[],
				null,
				'first',
				done
			);
		});

		it('sendQuery', function(done) {
			test(
				instance.sendQuery,
				['SELECT 1'],
				[],
				'query',
				done
			);
		});

		it('upgradeSchema', function(done) {
			test(
				instance.upgradeSchema,
				[],
				null,
				'first',
				done
			);
		});

	});

	describe('addTableTrigger', function() {
		var tableName = 'table',
			queueName = 'queue',
			escaped = {
				table: tableName,
				queueName: queueName
			};

		var instance;

		beforeEach(function() {
			instance = subject.PgQDatabaseAPI('a');
		});

		it('succeeds', function(done) {
			db.query = sinon.stub().withArgs(
				sinon.match.string,
				[],
				sinon.match.func
			).yields();

			db.query.first = sinon.stub().withArgs(
				sinon.match.string,
				[tableName, queueName],
				sinon.match.func
			).yields(null, escaped);

			instance.addTableTrigger(tableName, queueName, function(err, data) {
				assert.ok(!(err));
				assert.equal(data, true);
				done();
			});
		});

		it('fails when escaping fails', function(done) {
			var expectedErr = new Error('err');

			db.query.first = sinon.stub().withArgs(
				sinon.match.string,
				[tableName, queueName],
				sinon.match.func
			).yields(expectedErr);

			instance.addTableTrigger(tableName, queueName, function(err, data) {
				assert.equal(err, expectedErr);
				assert.ok(!data);
				done();
			});
		});

		it('fails when query gets error', function(done) {
			var expectedErr = new Error('err');
			db.query = sinon.stub().withArgs(
				sinon.match.string,
				[],
				sinon.match.func
			).yields(expectedErr);

			db.query.first = sinon.stub().withArgs(
				sinon.match.string,
				[tableName, queueName],
				sinon.match.func
			).yields(null, escaped);

			instance.addTableTrigger(tableName, queueName, function(err, data) {
				assert.equal(err, expectedErr);
				assert.ok(!data);
				done();
			});
		});

		it('reports success when query fails because trigger exists', function(done) {
			var DUPLICATE_OBJECT = '42710',
				expectedErr = new Error('err');
			expectedErr.code = DUPLICATE_OBJECT;
			db.query = sinon.stub().withArgs(
				sinon.match.string,
				[],
				sinon.match.func
			).yields(expectedErr);

			db.query.first = sinon.stub().withArgs(
				sinon.match.string,
				[tableName, queueName],
				sinon.match.func
			).yields(null, escaped);

			instance.addTableTrigger(tableName, queueName, function(err, data) {
				assert.ok(!err);
				assert.equal(data, false);
				done();
			});
		});

	});



});