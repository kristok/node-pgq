var sinon = require('sinon'),
	proxyrequire = require('proxyquire'),
	assert = require('assert');

describe('setup.js', function() {
	var subject,
		instance,
		dbapi,
		dbapiModule,
		fake;

	beforeEach(function() {
		dbapi = {
		};
		dbapiModule = {
			PgQDatabaseAPI: sinon.stub().returns(dbapi)
		};
		fake = {
			dbapi: dbapiModule,
			'./dbapi': dbapiModule
		};
		subject = proxyrequire('../lib/setup', fake);
	});

	describe('watchTables', function() {
		beforeEach(function() {
			instance = subject('dbConStr');
			dbapi.createQueue = sinon.stub().yields();
			dbapi.addTableTrigger = sinon.stub().yields();
		});

		it('should succeed with list of tables', function(done) {
			instance.watchTables(['a','b'], 'queue', function(err) {
				assert.ok(!err);
				done();
			});
		});

		it('should succeed with one table', function(done) {
			instance.watchTables('table', 'queue', function(err) {
				assert.ok(!err);
				done();
			});
		});
	});
});