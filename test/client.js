var riak = require('riak-js');

module.exports = {
  get: function(db, type, id, callback) {
    var namespace = [db, type].join('/').replace('/', '.');

    var client = riak.getClient({host: 'localhost', port: 8098});

    client.get(namespace, id, function(err, res) {
      if (res && res.statusCode === 404) {
        res = null;
      }
      callback(err, res || null);
    });
  },

  set: function(db, type, id, data, callback) {
    var namespace = [db, type].join('/').replace('/', '.');

    var client = riak.getClient({host: 'localhost', port: 8098});

    client.save(namespace, id, data, function(err, res) {
      callback(err, res || null);
    });
  },

  del: function(db, type, id, callback) {
    var namespace = [db, type].join('/').replace('/', '.');

    var client = riak.getClient({host: 'localhost', port: 8098});

    client.remove(namespace, id, function(err, res) {
      if (res && res.statusCode === 404) {
        res = null;
      }
      callback(err, res || null);
    });
  },

  exists: function(db, type, id, callback) {
    var namespace = [db, type].join('/').replace('/', '.');

    var client = riak.getClient({host: 'localhost', port: 8098});

    client.exists(namespace, id, function(err, res) {
      if (res && res.statusCode === 404) {
        res = null;
      }
      callback(err, res || null);
    });
  }
};
