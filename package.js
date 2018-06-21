Package.describe({
  name: 'teamgrid:job-queue',
  version: '0.2.0',
  // Brief, one-line summary of the package.
  summary: 'Package to create and work on jobs and sync them over oplog',
  // URL to the Git repository containing the source code for this package.
  git: 'https://github.com/teamgrid/meteor-job-queue.git',
  // By default, Meteor will default to using README.md for documentation.
  // To avoid submitting documentation, set this field to null.
  documentation: 'README.md'
});

Npm.depends({
  'digitaledgeit-observable-collection': '0.1.1',
})

Package.onUse(function(api) {
  api.versionsFrom('1.3.3.1');
  api.use('ecmascript');
  api.use('mongo');
  api.use('dburles:mongo-collection-instances@0.3.5');
  api.mainModule('src/main.js', 'server');
});
