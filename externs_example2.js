var levelup  = require('./')
  , rimraf   = require('rimraf')
  , location = require('path').join(require('os').tmpDir(), 'prefixed1.db')

    // this is our "plugin" and it can be applied to any number of databases independently
    // note this is a very naive implementation, it ignores batch() operations and doesn't
    // index them and ReadStreams will expose all entries, primary and indexes
  , indexer = {

        // on database instantiation we extract the 'indexProperties' from the options object
        // and attach a new `.getBy()` method to the database object that can
        // fetch by index values
        constructor: function (options, next) {
          this._indexProperties = (options && options.indexProperties) || []
          this.getBy = function (property, key, callback) {
            // turn it into a get() on an index with the given key
            this.get('index:' + property + ':' + JSON.stringify(key), function (err, value) {
              if (err) return callback(err)
              // bingo! get the primary entry now
              this.get(value, callback)
            }.bind(this))
          }
          next(options)
        }

        // all puts get turned into batches so we can store indexes too
      , put: function (key, value, options, callback) {
          value = JSON.parse(value) // undo the encoding
          var batch = [ { type: 'put', key: 'primary:' + key, value: value } ]
          // for each property we want to index, store an index also
          this._indexProperties.forEach(function (prop) {
            if (value[prop])
              batch.push({ type: 'put', key: 'index:' + prop + ':' + JSON.stringify(value[prop]), value: key })
          })
          return this.batch(batch, options, callback)
          // next() not called, real put() is never used
        }

        // fix up get()s so we can fetch indexes internally but external calls
        // fetch the primary entry
      , get: function (key, options, valueEnc, callback, next) {
          if (!/^index:/.test(key)) key = 'primary:' + key
          next(key, options, valueEnc, callback)
        }
    }

    // some interesting data to store, top npm packages
  , entries = [
        {
            name    : 'underscore'
          , author  : 'jashkenas'
          , version : '1.4.4'
          , url     : 'https://github.com/documentcloud/underscore'
        }
      , {
            name    : 'async'
          , author  : 'caolan'
          , version : '0.2.6'
          , url     : 'https://github.com/caolan/async'
        }
      , {
            name    : 'request'
          , author  : 'mikeal'
          , version : '2.14.0'
          , url     : 'https://github.com/mikeal/request'
        }
      /* duplicate indexed properties are left as an exercise for the reader!
      , {
            name    : 'coffee-script'
          , author  : 'jashkenas'
          , version : '1.6.1'
          , url     : 'https://github.com/jashkenas/coffee-script'
        }
      */
      , {
            name    : 'express'
          , author  : 'tjholowaychuk'
          , version : '3.1.0'
          , url     : 'https://github.com/visionmedia/express'
        }
      , {
            name    : 'optimist'
          , author  : 'substack'
          , version : '0.3.5'
          , url     : 'https://github.com/substack/node-optimist'
        }
    ]

    // make a database
  , db = levelup('/tmp/indexer.db', {
        // inject the plugin
        use             : indexer
        // our plugin reads this option
      , indexProperties : [ 'author', 'version' ]
      , keyEncoding     : 'utf8'
      , valueEncoding   : 'json'
    })

    // store the entries
  , setup = function (callback) {
      var done = 0
      entries.forEach(function (entry) {
        db.put(entry.name, entry, function (err) {
          if (err) throw err
          if (++done == entries.length)
            callback()
        })
      })
    }

setup(function () {

  // a standard get() on a primary entry
  db.get('underscore', function (err, value) {
    if (err) throw err
    console.log('db.get("underscore") =', JSON.stringify(value))
  })

  // some gets by indexed properties
  db.getBy('author', 'jashkenas', function (err, value) {
    if (err) throw err
    console.log('db.getBy("author", "jashkenas") =', JSON.stringify(value))
  })

  db.getBy('author', 'mikeal', function (err, value) {
    if (err) throw err
    console.log('db.getBy("author", "mikeal") =', JSON.stringify(value))
  })

  db.getBy('version', '0.2.6', function (err, value) {
    if (err) throw err
    console.log('db.getBy("version", "0.2.6") =', JSON.stringify(value))
  })

})