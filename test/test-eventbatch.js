var sinon = require('sinon'),
	proxyrequire = require('proxyquire'),
	Event = require('../lib/event'),
	assert = require('assert');

describe('eventbatch.js', function() {
	var subject,
		mockData = [
		{
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
		{
			ev_id: '9',
			ev_time: new Date('Sun Aug 02 2015 18:14:04 GMT+0300 (EEST)'),
			ev_txid: '196333',
			ev_retry: null,
			ev_type: 'U:id',
			ev_data: 'id=2&user_id=3',
			ev_extra1: 'tablename',
			ev_extra2: null,
			ev_extra3: null,
			ev_extra4: null
		}],
		batchId = 10,
		queueName = 'my_queue',
		consumer,
		fake;

	beforeEach(function() {
		subject = proxyrequire('../lib/eventbatch', {});
		consumer = {
			emit: sinon.spy(),
			dbapi: {
				finishBatch: null,
				eventTagRetry: null,
				eventTagRetrySeconds: null
			}
		};
	});

	it('initializes batch correctly', function(done) {
		var batch = new subject(consumer, queueName, batchId, mockData);

		assert.deepEqual(batch.consumer, consumer);
		assert.equal(batch.queueName, queueName);
		assert.equal(batch.batchId, batchId);
		assert.equal(batch.unchangedIds, mockData.length);
		assert.deepEqual(batch.ids, {8: Event.UNTAGGED, 9: Event.UNTAGGED});

		done();
	});

	it('calls finishBatch when all events are tagged done', function(done) {
		consumer.dbapi.finishBatch = sinon.spy().withArgs(batchId);
		var batch = new subject(consumer, queueName, batchId, mockData);
		batch.tagDone('8');
		batch.tagDone('9');
		assert.ok(consumer.dbapi.finishBatch.calledOnce);
		done();
	});

	it('calls finishBatch when some events are tagged retry', function(done) {
		var expectedData = {finish_batch: 1};
		consumer.dbapi.finishBatch = sinon.stub().yields(null, expectedData);
		consumer.dbapi.eventTagRetrySeconds = sinon.stub().yields();

		var batch = new subject(consumer, queueName, batchId, mockData);
		batch.tagDone('8');
		batch.tagRetrySeconds('9', 5000, function(err, data) {
			assert.ok(consumer.dbapi.finishBatch.calledOnce);
			assert.equal(data, expectedData);
			done();
		});
	});

	it('calls the callback of retry in case it is provided', function(done) {
		consumer.dbapi.eventTagRetrySeconds = sinon.stub().yields();
		consumer.dbapi.finishBatch = sinon.stub().yields();

		var batch = new subject(consumer, queueName, batchId, mockData);
		batch.tagDone('8');
		batch.tagRetrySeconds('9', 5000, function() {
			done();
		});
	});


});