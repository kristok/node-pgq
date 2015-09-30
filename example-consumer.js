var pgq = require('./index.js'),
	config = {
		consumerName: 'example_consumer',
		source: {
			database: 'postgres://dev:dev@localhost:5432/wtaibu_monitooring',
			//database: 'postgres://postgres@192.168.99.100:32768/postgres',
			queue: 'table.changes'
		},
		logDebug: true
	},
	consumer,
	pgqSetup,
	ticker;

pgqSetup = pgq.Setup(config.source.database);
pgqSetup.on('log', function(msg) {
	console.log('SETUP: '+msg);
});

pgqSetup.watchTables('cloudplay.playlists', config.source.queue, function(err) {
	if (err) {
		console.log('failed to set up watched tables');
		console.log(err);
		process.exit(1);
	}

	consumer = new pgq.Consumer(config);
	consumer.connect();

	consumer.on('event', function(ev) {
		console.log('CONSUMER: received event with id :'+ev.id);
		console.log(ev);

		ev.tagDone();
	});

	consumer.on('error', function(err) {
		console.log('CONSUMER: got error');
		console.log(err);
	});

	consumer.on('connected', function() {
		console.log('CONSUMER: connected');
	});

	consumer.on('log', function(msg) {
		console.log('CONSUMER: '+msg);
	});

	ticker = pgq.Ticker({database: config.source.database});
	ticker.start();

});