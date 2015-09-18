var sinon = require('sinon'),
	assert = require('assert'),
	proxyrequire = require('proxyquire'),
	path = require('path'),
	subjectPath = '../lib/files';

describe('files.js', function() {
	var subject,
		fake;

	describe('getSql', function() {
		it('should return concatenated files as sql', function() {
			var sql,
				readFileSync = sinon.stub().returns('data'),
				concatDir = sinon.spy();

			fake = {
				fs: {
					readFileSync: readFileSync
				}
			};

			subject = proxyrequire(subjectPath, fake);
			subject.getPath = sinon.stub().returns('/project/lib');
			subject.concatDir = concatDir;

			sql = subject.getSql();

			assert.ok(readFileSync.calledWith('/project/sql/structure/roles.sql'));
			assert.ok(readFileSync.calledWith('/project/sql/structure/tables.sql'));
			assert.ok(concatDir.calledWith('/project/sql/functions'));
			assert.ok(concatDir.calledWith('/project/sql/triggers'));
		});
	});

	describe('concatDir', function() {
		var content,
			readdirSync = sinon.stub().returns(['file1', 'file2', 'file3']),
			readFileSync = sinon.stub().returns('file');

		fake = {
			fs: {
				readdirSync: readdirSync,
				readFileSync: readFileSync
			}
		};

		subject = proxyrequire(subjectPath, fake);
		subject.getPath = sinon.stub().returns('/project');

		content = subject.concatDir('/somedir');

		assert.ok(readdirSync.calledWith('/somedir'));
		assert.ok(readFileSync.calledWith('/somedir/file1'));
		assert.ok(readFileSync.calledWith('/somedir/file2'));
		assert.ok(readFileSync.calledWith('/somedir/file3'));
		assert.equal(content, 'file\nfile\nfile');
	});

	describe('getPath', function() {
		var expectedPath = path.join(__dirname, '../lib');
		subject = proxyrequire(subjectPath, {});
		assert.equal(subject.getPath(), expectedPath);
	});
});