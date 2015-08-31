var sinon = require('sinon'),
	proxyrequire = require('proxyquire'),
	assert = require('assert'),
	clock;

var validateThrow = function(subject, config, match) {
	assert.throws(
		function() {
			subject(config);
		},
		match
	);
};

describe('ticker.js', function() {
	var subject,
		instance,
		dbapi,
		dbapiModule,
		fake,
		logDebugCounter = 0;

	beforeEach(function() {
		dbapi = {};
		dbapiModule = {
			PgQDatabaseAPI: sinon.stub().returns(dbapi)
		};
		fake = {
			'./dbapi': dbapiModule,
			dbapi: dbapiModule
		};
		subject = proxyrequire('../lib/ticker', fake);
	});

	describe('initialization', function() {
		it('throws when database is missing', function() {
			validateThrow(subject, {}, /database must be given/);
		});

		it('builds a new dbapi object', function() {
			subject({database: 'fake'});
			assert.ok(dbapiModule.PgQDatabaseAPI.calledWith('fake'));
		});
	});

	describe('tick', function() {
		beforeEach(function() {
			var useDebug = !!(logDebugCounter++);
			clock = sinon.useFakeTimers();
			instance = subject({database: 'fake', logDebug: useDebug});
		});

		afterEach(function() {
			clock.restore();
			instance.stop();
		});

		describe('regular', function() {
			beforeEach(function() {
				instance.maintenance = sinon.stub();
				instance.retry = sinon.stub();
			});

			it('should be called at set intervals', function() {
				dbapi.createTick = sinon.stub().yields();
				instance.start();
				clock.tick(instance.tickerPeriod * 2);
				assert.ok(dbapi.createTick.calledThrice);
			});

			it('should emit on error', function(done) {
				var expectedErr = new Error('err');
				dbapi.createTick = sinon.stub().yields(expectedErr);
				instance.addListener('error', function(err) {
					assert.equal(err, expectedErr);
					done();
				});
				instance.start();
			});
		});

		describe('retry', function() {
			beforeEach(function() {
				instance.maintenance = sinon.stub();
				instance.tick = sinon.stub();
			});

			it('should be called at set intervals', function() {
				dbapi.reinsertRetryEvents = sinon.stub().yields();
				instance.start();
				clock.tick(instance.retryPeriod * 2);
				assert.ok(dbapi.reinsertRetryEvents.calledThrice);
			});

			it('should emit on error', function(done) {
				var expectedErr = new Error('err');
				dbapi.reinsertRetryEvents = sinon.stub().yields(expectedErr);
				instance.addListener('error', function(err) {
					assert.equal(err, expectedErr);
					done();
				});
				instance.start();
			});
		});

		describe('maintenance', function() {
			var ops = [
				{func_name: 'f1', func_arg: 'arg'},
				{func_name: 'f2', func_arg: 'arg'}
			];

			it('should be called at set intervals', function() {
				instance.tick = sinon.stub();
				instance.retry = sinon.stub();

				dbapi.getMaintenanceOperations = sinon.stub().yields(null, ops);
				dbapi.maintenance = sinon.stub().yields();
				instance.start();
				clock.tick(instance.maintenancePeriod);

				assert.equal(dbapi.maintenance.callCount, 4);
			});


			it('should not crash when no ops are returned', function() {
				instance.tick = sinon.stub();
				instance.retry = sinon.stub();

				dbapi.getMaintenanceOperations = sinon.stub().yields(null, null);
				dbapi.maintenance = sinon.stub().yields();
				instance.start();
				clock.tick(instance.maintenancePeriod);

				assert.equal(dbapi.maintenance.callCount, 0);
			});

			describe('should fail when db calls fail and emit error', function() {
				var expectedErr;

				beforeEach(function() {
					expectedErr = new Error('err');
					instance.tick = sinon.stub().yields();
					instance.retry = sinon.stub().yields();
				});

				it('for get maintenance operations', function(done) {
					dbapi.getMaintenanceOperations = sinon.stub().yields(expectedErr);

					instance.addListener('error', function(err) {
						assert.equal(err, expectedErr);
						done();
					});

					instance.start();
				});

				it('for perform maintenance operation', function(done) {

					dbapi.getMaintenanceOperations = sinon.stub().yields(null, ops);
					dbapi.maintenance = sinon.stub().yields(expectedErr);

					instance.addListener('error', function(err) {
						assert.equal(err, expectedErr);
						done();
					});

					instance.start();
				});
			});

		});
	});
});