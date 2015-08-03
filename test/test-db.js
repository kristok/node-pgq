var sinon = require('sinon'),
	proxyrequire = require('proxyquire'),
	assert = require('assert');

describe('db.js', function() {
	var fake,
		subject;

	beforeEach(function() {
		fake = {
			pg: {}
		};
		subject = proxyrequire('../lib/db', fake);
	});

	it('returns error when it cant get client from pool', function(done) {
		var expectedErr = new Error();
		fake.pg.connect = sinon.stub().yields(expectedErr);

		subject({}).query('', [], function(err) {
			assert.equal(err, expectedErr);
			done();
		});
	});

	it('returns error when query fails, and releases client', function(done) {
		var expectedErr = new Error(),
			expectedDone = sinon.spy(),
			client = {};
		client.query = sinon.stub().yields(expectedErr);
		fake.pg.connect = sinon.stub().yields(null, client, expectedDone);

		subject({}).query('', [], function(err) {
			assert.equal(err, expectedErr);
			assert.ok(expectedDone.calledOnce);
			done();
		});
	});

	it('returns result when query succeeds and releases client', function(done) {
		var expectedResult = {
				rows: [{hi: 'me'}]
			},
			expectedDone = sinon.spy(),
			client = {};

		client.query = sinon.stub().yields(null, expectedResult);
		fake.pg.connect = sinon.stub().yields(null, client, expectedDone);

		subject({}).query('', [], function(err, result) {
			assert.equal(result, expectedResult.rows);
			assert.ok(expectedDone.calledOnce);
			done();
		});
	});

	it('throws when query is not string', function(done) {
		assert.throws(
			function() {
				subject({}).query({not_string: 1});
			},
			/string/
		);
		done();
	});


	it('throws when params is not an array', function(done) {
		assert.throws(
			function() {
				subject({}).query('', 'not array');
			},
			/array/
		);
		done();
	});

	it('throws when callback is not a function', function(done) {
		assert.throws(
			function() {
				subject({}).query('', [], {});
			},
			/callback/
		);
		done();
	});

	it('does not crash without callback', function(done) {
		var expectedDone = sinon.spy(),
			client = {};

		client.query = sinon.stub().yields(null, {rows:[]});
		fake.pg.connect = sinon.stub().yields(null, client, expectedDone);

		subject({}).query('', []);
		setTimeout(done, 1);

	});
});

