var sinon = require('sinon'),
	assert = require('assert'),
	proxyrequire = require('proxyquire');

describe('installpgq.js', function() {
	var subject,
		api,
		setup,
		fake;

	beforeEach(function() {
		api = {};
		fake = {
			'./files': {
				getSql: sinon.stub().returns('SQL')
			}
		};
		setup = {
			emit: sinon.spy()
		};
		subject = proxyrequire('../lib/installpgq', fake);
	});

	it('should skip if pgq schema is installed', function(done) {
		api.checkIfPgQSchemaExists = sinon.stub().yields(null, {exists: true});
		subject(api, setup, function(err) {
			assert.ok(!err);
			done();
		});
	});

	it('should run sql when pgq schema does not exist', function(done) {
		api.checkIfPgQSchemaExists = sinon.stub().yields();
		api.sendQuery = sinon.stub().yields();
		subject(api, setup, function(err) {
			assert.ok(!err);
			assert.ok(api.sendQuery.withArgs('SQL').calledOnce);
			done();
		});
	});

	it('should fail on error', function(done) {
		var expectedErr = new Error('err');
		api.checkIfPgQSchemaExists = sinon.stub().yields(expectedErr);
		subject(api, setup, function(err) {
			assert.equal(err, expectedErr);
			done();
		});
	});

	it('should fail on apply sql error', function(done) {
		var expectedErr = new Error('err');
		api.checkIfPgQSchemaExists = sinon.stub().yields();
		api.sendQuery = sinon.stub().yields(expectedErr);
		subject(api, setup, function(err) {
			assert.equal(err, expectedErr);
			done();
		});
	});
});