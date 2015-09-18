var fs = require('fs'),
	path = require('path'),
	_ = require('lodash');

exports.getPath = function() {
	return __dirname;
};

exports.concatDir = function(dir) {
	var fileList = fs.readdirSync(dir),
		content = _.map(fileList, function(file) {
			return fs.readFileSync(path.join(dir, file));
		});

	return content.join('\n');
};

exports.getSql = function() {
	var statements,
		rolesFile = path.join(exports.getPath(), '../sql/structure/roles.sql'),
		tablesFile = path.join(exports.getPath(), '../sql/structure/tables.sql'),
		functionsDir = path.join(exports.getPath(), '../sql/functions'),
		triggersDir = path.join(exports.getPath(), '../sql/triggers');

	statements = [
		fs.readFileSync(rolesFile),
		fs.readFileSync(tablesFile),
		exports.concatDir(functionsDir),
		exports.concatDir(triggersDir)
	].join('\n');

	return statements;
};