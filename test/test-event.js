var sinon = require('sinon'),
	proxyrequire = require('proxyquire'),
	assert = require('assert');

describe('event.js', function() {
	var subject,
		mockData = {
			id: '8',
			time: new Date('Sun Aug 02 2015 18:14:04 GMT+0300 (EEST)'),
			txid: '196333',
			retry: null,
			type: 'log_json',
			data: '{"id": 1, "user_id": 2}',
			prev_data: '{"id": 1, "user_id": 3}',
			table: 'sometable',
			primary_keys: 'pk,ids',
			operation: 'UPDATE'
		},
		batchId = 10,
		queueName = 'my_queue',
		batchTracker;

	beforeEach(function() {
		subject = proxyrequire('../lib/event', {});
		batchTracker = {
			tagDone: sinon.spy(),
			tagRetrySeconds: sinon.spy(),
			tagRetryTimestamp: sinon.spy(),
			tagBatchDone: sinon.spy(),
			failBatch: sinon.spy()
		};
	});

	it('has the correct attributes', function(done) {
		var ev = new subject(batchTracker, queueName, batchId, mockData),
			expectedData = {
				id: '1',
				user_id: '2'
			},
			expectedPrevData = {
				id: '1',
				user_id: '3'
			};

		assert.equal(ev.queueName, queueName);
		assert.equal(ev.batchId, batchId);
		assert.equal(ev.id, mockData.id);
		assert.equal(ev.time, mockData.time);
		assert.deepEqual(ev.data, expectedData);
		assert.deepEqual(ev.prev_data, expectedPrevData);
		assert.deepEqual(ev.primary_keys, ['pk', 'ids']);
		assert.equal(ev.state, subject.UNTAGGED);
		done();
	});

	it('forwards tagDone calls to batch', function(done) {
		var ev = new subject(batchTracker, queueName, batchId, mockData);
		ev.tagDone();
		assert.ok(batchTracker.tagDone.calledWith(mockData.id));
		done();
	});

	describe('tagRetry', function() {
		it('forwards calls with seconds to batch', function(done) {
			var ev = new subject(batchTracker, queueName, batchId, mockData);
			ev.tagRetry(30);
			assert.ok(batchTracker.tagRetrySeconds.calledWith(mockData.id));
			done();
		});

		it('forwards calls with timestamps (Date) to batch', function(done) {
			var ev = new subject(batchTracker, queueName, batchId, mockData),
				retryTime = new Date((new Date()).getTime() + 60000);

			ev.tagRetry(retryTime);
			assert.ok(batchTracker.tagRetryTimestamp.calledWith(mockData.id));
			done();
		});

		it('forwards calls with timestamps (String) to batch', function(done) {
			var ev = new subject(batchTracker, queueName, batchId, mockData),
				retryTime = '2016-08-01 11:11:11';

			ev.tagRetry(retryTime);
			assert.ok(batchTracker.tagRetryTimestamp.calledWith(mockData.id));
			done();
		});

		it('fails calls when argument type is wrong', function(done) {
			var ev = new subject(batchTracker, queueName, batchId, mockData),
				retryTime = {};

			assert.throws(
				function() {
					ev.tagRetry(retryTime);
				},
				/needs seconds or date/
			);
			done();
		});
	});

	it('forwards failBatch calls to batch', function(done) {
		var ev = new subject(batchTracker, queueName, batchId, mockData);
		ev.failBatch();
		assert.ok(batchTracker.failBatch.called);
		done();
	});

	it('throws when type is not log_json', function(done) {
		assert.throws(
			function() {
				new subject(batchTracker, queueName, batchId, {type: 'wrong' });
			},
			/log_json/
		);
		done();
	});

});