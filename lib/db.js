var pg = require('pg'),
	_ = require('lodash');

module.exports = function(conString) {
	var self = {
		conString: conString
	};

	self.query = function(query, params, callback) {
		// prevent typos
		if (callback && !_.isFunction(callback)) {
			throw new Error('db.query callback must be a function');
		}

		if (!_.isString(query)) {
			throw new Error('db.query is not a string');
		}

		if (!_.isArray(params)) {
			throw new Error('db.query params must be an array');
		}

		pg.connect(self.conString, function(err, client, done) {
			// could not get client from pool
			if (err) return callback(err);

			client.query(query, params, function(err, result) {
				done();
				if (callback) {
					callback(err, result && result.rows);
				}
			});
		});
	};

	self.query.first = function(query, params, callback) {
		self.query(query, params, function(err, result) {
			if (err) return callback(err);
			callback(null, result ? result[0] : null);
		});
	};

	return self;
};
