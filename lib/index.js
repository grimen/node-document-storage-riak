require('sugar')
var fun = require('funargs'),
    util = require('util');

// HACK: ...until Node.js `require` supports `instanceof` on modules loaded more than once. (bug in Node.js)
var Storage = global.NodeDocumentStorage || (global.NodeDocumentStorage = require('node-document-storage'));

// -----------------------
//  DOCS
// --------------------
//  - https://github.com/mostlyserious/riak-js

// -----------------------
//  Constructor
// --------------------

// new Riak ();
// new Riak (options);
// new Riak (url);
// new Riak (url, options);
function Riak () {
  var self = this;

  self.klass = Riak;
  self.klass.super_.apply(self, arguments);
}

util.inherits(Riak, Storage);

// -----------------------
//  Class
// --------------------

Riak.defaults = {
  url: process.env.RIAK_URL || 'http://localhost:8098/{db}-{env}'.assign({db: 'default', env: (process.env.NODE_ENV || 'development')}),
  options: {
    client: {
      headers: {}
    }
  }
};

Riak.url = Riak.defaults.url;
Riak.options = Riak.defaults.options;

Riak.reset = Storage.reset;

// -----------------------
//  Instance
// --------------------

// #connect ()
Riak.prototype.connect = function() {
  var self = this;

  self._connect(function() {
    var riak = require('riak-js');

    self.client = riak.getClient({host: self.options.server.hostname, port: self.options.server.port});

    if (self.options.server.username || self.options.server.password) {
      self.options.client.headers = self.options.client.headers.merge({
        headers: {
          Authorization: 'Basic ' + new Buffer(self.options.server.username + '.' + self.options.server.password).toString('base64')
        }
      });
    }

    self.client.save('node-document', 'auth', {}, self.options.client.headers, function(err, res) {
      self.emit('ready', err);
    });
  });
};

// #key (key)
Riak.prototype.key = function(key) {
  var self = this, parts = key.split('/');
  return {
    db: self.options.server.db.replace(/^\//, ''),
    ns: (self.options.server.db + '/' + parts[0]).replace(/^\//, ''),
    key: key,
    type: parts[0],
    id: parts[1]
  };
};

// #set (key, value, [options], callback)
// #set (keys, values, [options], callback)
Riak.prototype.set = function() {
  var self = this;

  self._set(arguments, function(key_values, options, done, next) {
    key_values.each(function(key, value) {
      var resource = self.key(key);

      self.client.save(resource.ns.replace('/', '.'), resource.id, value, self.options.client.headers, function(error, response) {
        next(key, error, !error, response);
      });
    });
  });
};

// #get (key, [options], callback)
// #get (keys, [options], callback)
Riak.prototype.get = function() {
  var self = this;

  self._get(arguments, function(keys, options, done, next) {
    keys.each(function(key) {
      var resource = self.key(key);

      self.client.get(resource.ns.replace('/', '.'), resource.id, self.options.client.headers, function(error, response) {
        var result = response;

        if (response && response.statusCode === 404) {
          result = null;
        }

        next(key, error, result, response);
      });
    });
  });
};

// #del (key, [options], callback)
// #del (keys, [options], callback)
Riak.prototype.del = function() {
  var self = this;

  self._del(arguments, function(keys, options, done, next) {
    keys.each(function(key) {
      var resource = self.key(key);

      self.client.remove(resource.ns.replace('/', '.'), resource.id, self.options.client.headers, function(error, response) {
        var result = !error;

        if (response && response.statusCode === 404) {
          result = false;
        }

        next(key, error, result, response);
      });
    });
  });
};

// #exists (key, [options], callback)
// #exists (keys, [options], callback)
Riak.prototype.exists = function() {
  var self = this;

  self._exists(arguments, function(keys, options, done, next) {
    keys.each(function(key) {
      var resource = self.key(key);

      self.client.exists(resource.ns.replace('/', '.'), resource.id, self.options.client.headers, function(error, response) {
        next(key, error, !!response, response);
      });
    });
  });
};

// -----------------------
//  Export
// --------------------

module.exports = Riak;
