var leveldb = require('./')
  , rimraf = require('rimraf')
  , location = require('path').join(require('os').tmpDir(), 'prefixed1.db')

    // the externs plugin to send to leveldb's `use`
  , pfxextern = function (pfx) {
      return {
          inKey: function (key) {
            return pfx + key.toString()
          }
        , outKey: function (key) {
            return key.toString().replace(new RegExp('^' + pfx), '')
          }
      }
    }

    // a plugin that messes with the `put` method to drop a particular key
  , sneakyextern = {
        put: function (key, options, valueEnc, callback, next) {
          if (key == 'foo2') {
            // bypass the real get() operation, jump straight to the user callback
            return callback()
          }
          // internal next() callback for the extern chain
          next(key, options, valueEnc, callback)
        }
    }

    // put keys to a database, using the provided `use`
  , put = function (use, callback) {
      rimraf(location, function () {
        var db = leveldb(location, { use: use })
        db.put('foo1', 'bar2', function () {
          db.put('foo2', 'bar2', function () {
            db.put('foo3', 'bar3', function () {
              db.close(callback)
            })
          })
        })
      })
    }

    // print key/value pairs from the database using the provided `use
  , print = function (use, callback) {
      var db = leveldb(location, { use: use })
      db.readStream()
        .on('data', console.log.bind(console, '\t'))
        .on('close', function () {
          db.close(callback)
        })
    }

    // instantiated versions of the plugin with specific prefixes
  , aprefix = pfxextern('aprefix:')
  , secondprefix = pfxextern('secondprefix:')

  , runprefixed = function (callback) {
      console.log('Writing 3 entries to db, entries prefixed with `aprefix:`')
      put(aprefix, function () {
        console.log('Printing db, with `use`')
        print(aprefix, function () {
          console.log('Printing db, no `use` (i.e. what\'s *actually* in there)')
          print(null, callback)
        })
      })
    }

  , rundoubleprefixed = function (callback) {
      console.log('Writing 3 entries to db, entries prefixed with `aprefix:` & `secondprefix:`')
      put([ secondprefix, aprefix ], function () {
        console.log('Printing db, with `use`')
        print([ secondprefix, aprefix ], function () {
          console.log('Printing db, no `use` (i.e. what\'s *actually* in there)')
          print(null, callback)
        })
      })
    }

  , runsneaky = function (callback) {
      console.log('Writing 3 entries to db, with `sneakyextern` plugin')
      put(sneakyextern, function () {
        console.log('Printing db')
        print(null, callback)
      })
    }

runprefixed(function () {
  rundoubleprefixed(function () {
    runsneaky()
  })
})