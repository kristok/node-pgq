var sinon = require('sinon'),
	proxyrequire = require('proxyquire'),
	assert = require('assert');

describe('event.js', function() {
	var subject,
		mockData = {
			ev_id: '8',
			ev_time: new Date('Sun Aug 02 2015 18:14:04 GMT+0300 (EEST)'),
			ev_txid: '196333',
			ev_retry: null,
			ev_type: 'U:id',
			ev_data: 'id=1&user_id=2',
			ev_extra1: 'tablename',
			ev_extra2: null,
			ev_extra3: null,
			ev_extra4: null
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
			tagBatchDone: sinon.spy()
		};
	});

	it('has the correct attributes', function(done) {
		var ev = new subject(batchTracker, queueName, batchId, mockData),
			expectedData = {
				id: '1',
				user_id: '2'
			};

		assert.equal(ev.queueName, queueName);
		assert.equal(ev.batchId, batchId);
		assert.equal(ev.id, mockData.ev_id);
		assert.equal(ev.extra1, mockData.ev_extra1);
		assert.equal(ev.time, mockData.ev_time);
		assert.deepEqual(ev.data, expectedData);
		assert.equal(ev.state, subject.UNTAGGED);
		done();
	});

	it('forwards tagDone calls to batch', function(done) {
		var ev = new subject(batchTracker, queueName, batchId, mockData);
		ev.tagDone();
		assert.ok(batchTracker.tagDone.calledWith(mockData.ev_id));
		done();
	});

	it('forwards tagRetry calls with seconds to batch', function(done) {
		var ev = new subject(batchTracker, queueName, batchId, mockData);
		ev.tagRetry(30);
		assert.ok(batchTracker.tagRetrySeconds.calledWith(mockData.ev_id));
		done();
	});

	it('forwards tagRetry calls with timestamps to batch', function(done) {
		var ev = new subject(batchTracker, queueName, batchId, mockData),
			retryTime = new Date((new Date()).getTime() + 60000);

		ev.tagRetry(retryTime);
		assert.ok(batchTracker.tagRetryTimestamp.calledWith(mockData.ev_id));
		done();
	});


});