var pgq = require('./index.js'),
	config = {
		consumerName: 'example_consumer',
		source: {
			database: 'postgres://dev:dev@localhost:5432/wtaibu_monitooring',
			queue: 'table.changes'
		},
		logDebug: true
	},
	consumer,
	ticker;

pgq.Setup(config.source.database)
.watchTables('cloudplay.playlists', config.source.queue, function(err) {
	if (err) {
		console.log('failed to set up watched tables ', err);
		process.exit(1);
	}

	consumer = new pgq.Consumer(config);
	consumer.connect();

	consumer.on('event', function(ev) {
		console.log('received event with id :'+ev.id);
		console.log(ev);

		ev.tagDone();
	});

	consumer.on('error', function(err) {
		console.log('got error');
		console.log(err);
	});

	consumer.on('connected', function() {
		console.log('consumer connected');
	});

	consumer.on('log', function(msg) {
		console.log(msg);
	});

	ticker = new pgq.Ticker({database: config.source.database});
	ticker.start();

});
