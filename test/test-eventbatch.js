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
		consumer;

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
		batch.tagRetrySeconds('9', 5000, function(err) {
			assert.ok(consumer.dbapi.finishBatch.calledOnce);
			assert.ok(!err);
			assert.ok(consumer.emit.calledWith('batchProcessed'));
			done();
		});
	});

	it('emits error when database call fails', function(done) {
		var expectedErr = new Error('err');
		consumer.dbapi.finishBatch = sinon.stub().yields(expectedErr);
		consumer.dbapi.eventTagRetrySeconds = sinon.stub().yields();

		var batch = new subject(consumer, queueName, batchId, mockData);
		batch.tagDone('8');
		batch.tagDone('9', function(err) {
			assert.equal(err, expectedErr);
			assert.ok(consumer.emit.calledWith('error', err));
			done();
		});
	});

	it('emits error when response did not contain finish_batch', function(done) {
		consumer.dbapi.finishBatch = sinon.stub().yields(null, {});
		consumer.dbapi.eventTagRetrySeconds = sinon.stub().yields();

		var batch = new subject(consumer, queueName, batchId, mockData);
		batch.tagDone('8');
		batch.tagDone('9', function(err) {
			assert.ok(err);
			assert.ok(consumer.emit.called);
			assert.ok(err.message.indexOf('finish_batch') > -1);
			done();
		});
	});


	it('emits error when failBatch is called', function(done) {
		var batch = new subject(consumer, queueName, batchId, mockData);
		batch.failBatch();
		assert.ok(consumer.emit.calledWith('error', sinon.match.any));
		done();
	});

	it('tagRetry version with callback works', function(done) {
		consumer.dbapi.eventTagRetryTimestamp = sinon.stub().yields();
		consumer.dbapi.finishBatch = sinon.stub().yields(null, {finish_batch: true});

		var batch = new subject(consumer, queueName, batchId, mockData);
		batch.tagRetryTimestamp('8', new Date(), function() {
			// nothing here really, just wanted to trigger one more path
		});
		batch.tagRetryTimestamp('9', new Date(), function(err) {
			assert.ok(!err);
			done();
		});
	});

	it('tagRetry version with callback fails when batch is completed', function() {
		consumer.dbapi.eventTagRetryTimestamp = sinon.stub().yields();
		consumer.dbapi.finishBatch = sinon.stub().yields();

		var batch = new subject(consumer, queueName, batchId, mockData);
		batch.tagDone('8');
		batch.tagRetryTimestamp('9', new Date());

		assert.ok(consumer.emit.calledWith('error', sinon.match.any));
	});

	it('tagRetry version with callback fails when tagging event', function(done) {
		var expectedErr = new Error('err');
		consumer.dbapi.eventTagRetryTimestamp = sinon.stub().yields(expectedErr);

		var batch = new subject(consumer, queueName, batchId, mockData);
		batch.tagRetryTimestamp('8', new Date(), function(err) {
			assert.ok(err, expectedErr);
			done();
		});
	});


});