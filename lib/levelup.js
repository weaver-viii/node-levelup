/* Copyright (c) 2012-2013 LevelUP contributors
 * See list at <https://github.com/rvagg/node-levelup#contributing>
 * MIT +no-false-attribs License
 * <https://github.com/rvagg/node-levelup/blob/master/LICENSE>
 */

const leveldown    = require('leveldown')
    , EventEmitter = require('events').EventEmitter
    , inherits     = require('util').inherits
    , extend       = require('xtend')
    , Externr      = require('externr')

    , errors       = require('./errors')
    , ReadStream   = require('./read-stream')
    , writeStream  = require('./write-stream')
    , toEncoding   = require('./util').toEncoding
    , toSlice      = require('./util').toSlice
    , encodingOpts = require('./util').encodingOpts

    , externDefinitions = {
          wrap: [
              // sync methods
              'constructor'
            , 'isOpen'
            , 'isClosed'
            , 'createReadStream'
              // async methods
            , 'open'
            , 'close'
            , 'put'
            , 'get'
            , 'del'
            , 'batch'
            , 'approximateSize'
          ]
        , extend: [
              'inKey'
            , 'inValue'
          ]
        , extendReverse: [
              'outKey'
            , 'outValue'
          ]
      }

var defaultOptions = {
          createIfMissing : true
        , errorIfExists   : false
        , encoding        : 'utf8'
        , keyEncoding     : null
        , valueEncoding   : null
        , compression     : true
      }

  , globalUse = function (externs) {
      if (externs) {
        if (!defaultOptions.use)
          defaultOptions.use = []

        if (Array.isArray(externs))
          defaultOptions.use = defaultOptions.use.concat(externs)
        else
          defaultOptions.use.push(externs)
      }
    }

  , createLevelUP = function (location, options, callback) {

      // Possible status values:
      //  - 'new'     - newly created, not opened or closed
      //  - 'opening' - waiting for the database to be opened, post open()
      //  - 'open'    - successfully opened the database, available for use
      //  - 'closing' - waiting for the database to be closed, post close()
      //  - 'closed'  - database has been successfully closed, should not be
      //                 used except for another open() operation

      var status = 'new'
        , error
        , levelup

        , isOpen        = function () { return status == 'open' }
        , isOpening     = function () { return status == 'opening' }

        , keyEncoding   = function (o) { return o.keyEncoding || o.encoding }
        , valueEncoding = function (o) { return o.valueEncoding || o.encoding }

        , dispatchError = function (error, callback) {
            return callback ? callback(error) : levelup.emit('error', error)
          }
        , externs = Externr(externDefinitions)

        , getCallback = function (options, callback) {
            return typeof options == 'function' ? options : callback
          }

        , getOptions = function (options) {
            return typeof options == 'string' // just an encoding
              ? extend(encodingOpts[options] ||
                       encodingOpts[defaultOptions.encoding])
              : extend(levelup._options, options)
          }

      if (typeof options == 'function') {
        callback = options
        options  = {}
      }

      if (typeof location != 'string') {
        error = new errors.InitializationError(
            'Must provide a location for the database')
        if (callback)
          return callback(error)
        throw error
      }

      function LevelUP (location, options) {
        EventEmitter.call(this)
        this.setMaxListeners(Infinity)

        options = extend(defaultOptions, options)

        externs.$register(options.use)

        externs.constructor(
            this
          , [ options ]
          , function (options) {
              this._options = options
              Object.defineProperty(this, 'location', {
                  value: location
                , configurable: false
                , enumerable: true
                , writable: false
              })
            }
        )
      }

      inherits(LevelUP, EventEmitter)

      LevelUP.prototype.open = function (callback) {
        if (isOpen()) {
          if (callback)
            process.nextTick(callback.bind(null, null, this))
          return this
        }

        if (isOpening())
          return callback && levelup.once(
              'open'
            , callback.bind(null, null, this)
          )

        status = 'opening'

        externs.open(
            this
          , [ callback ]
          , function (callback) {
              var db = leveldown(this.location)

              db.open(this._options, function (err) {
                if (err) {
                  err = new errors.OpenError(err)
                  return dispatchError(err, callback)
                } else {
                  levelup._db = db
                  status = 'open'
                  if (callback)
                    callback(null, levelup)
                  levelup.emit('open')
                  levelup.emit('ready')
                }
              })
            }
        )

        var deferred = {}

        ;['get', 'put', 'batch', 'del', 'approximateSize']
          .forEach(function (name) {
            deferred[name] = function () {
              var args = Array.prototype.slice.call(arguments)
              levelup.once('ready', function () {
                levelup._db[name].apply(levelup._db, args)
              })
            }
          })

        this._db = deferred

        levelup.emit('opening')
      }

      LevelUP.prototype.close = function (callback) {
        if (isOpen()) {
          status = 'closing'
          externs.close(
              this
            , [ callback ]
            , function (callback) {
                this._db.close(function () {
                  status = 'closed'
                  this.emit('closed')
                  if (callback)
                    callback.apply(null, arguments)
                }.bind(this))
                this.emit('closing')
                this._db = null
              }
          )
        } else if (status == 'closed' && callback) {
          callback()
        } else if (status == 'closing' && callback) {
          levelup.once('closed', callback)
        } else if (isOpening()) {
          levelup.once('open', function () {
            levelup.close(callback)
          })
        }
      }

      LevelUP.prototype.isOpen = function () {
        return externs.isOpen(
            this
          , []
          , function () {
              return isOpen()
            }
        )
      }

      LevelUP.prototype.isClosed = function () {
        return externs.isOpen(
            this
          , []
          , function () {
              return (/^clos/).test(status)
            }
        )
      }

      LevelUP.prototype.get = function (key_, options, callback) {
        var key
          , keyEnc
          , valueEnc
          , err

        callback = getCallback(options, callback)

        if (!isOpening() && !isOpen()) {
          err = new errors.ReadError('Database is not open')
          return dispatchError(err, callback)
        }

        options  = getOptions(options, this._options)
        keyEnc   = options.keyEncoding   || options.encoding
        valueEnc = options.valueEncoding || options.encoding
        key      = externs.inKey(toSlice[keyEnc](key_))
        options.asBuffer = valueEnc != 'utf8' && valueEnc != 'json'

        externs.get(
            this
          , [ key, options, valueEnc, callback ]
          , function (key, options, valueEnc, callback) {
              this._db.get(key, options, function (err, value) {
                if (err) {
                  err = new errors.NotFoundError(
                      'Key not found in database [' + key_ + ']')
                  return dispatchError(err, callback)
                }
                if (callback)
                  callback(null, externs.outValue(toEncoding[valueEnc](value)))
              })
            }
        )
      }

      LevelUP.prototype.put = function (key_, value_, options, callback) {
        var err
          , key
          , value

        callback = getCallback(options, callback)

        if (!isOpening() && !isOpen()) {
          err = new errors.WriteError('Database is not open')
          return dispatchError(err, callback)
        }

        options = getOptions(options, this._options)
        key     = externs.inKey(toSlice[options.keyEncoding || options.encoding](key_))
        value   = externs.inValue(toSlice[options.valueEncoding || options.encoding](value_))

        externs.put(
            this
          , [ key, value, options, callback ]
          , function (key, value, options, callback) {
              this._db.put(key, value, options, function (err) {
                if (err) {
                  err = new errors.WriteError(err)
                  return dispatchError(err, callback)
                } else {
                  this.emit('put', key_, value_)
                  if (callback)
                    callback()
                }
              }.bind(this))
            }
        )
      }

      LevelUP.prototype.del = function (key_, options, callback) {
        var err
          , key

        callback = getCallback(options, callback)

        if (!isOpening() && !isOpen()) {
          err = new errors.WriteError('Database is not open')
          return dispatchError(err, callback)
        }

        options = getOptions(options, this._options)
        key     = externs.inKey(toSlice[options.keyEncoding || options.encoding](key_))

        externs.get(
            this
          , [ key, options, callback ]
          , function (key, options, callback) {
              this._db.del(key, options, function (err) {
                if (err) {
                  err = new errors.WriteError(err)
                  return dispatchError(err, callback)
                } else {
                  this.emit('del', key_)
                  if (callback)
                    callback()
                }
              }.bind(this))
            }
        )
      }

      LevelUP.prototype.batch = function (arr_, options, callback) {
        var keyEnc
          , valueEnc
          , err
          , arr

        callback = getCallback(options, callback)

        if (!isOpening() && !isOpen()) {
          err = new errors.WriteError('Database is not open')
          return dispatchError(err, callback)
        }

        options  = getOptions(options)
        keyEnc   = keyEncoding(options)
        valueEnc = valueEncoding(options)

        // If we're not dealing with plain utf8 strings or plain
        // Buffers then we have to do some work on the array to
        // encode the keys and/or values. This includes JSON types.
        if ((keyEnc != 'utf8' && keyEnc != 'binary')
            || (valueEnc != 'utf8' && valueEnc != 'binary')) {

          arr = arr_.map(function (e) {
            if (e.type !== undefined && e.key !== undefined) {
              var o = {
                  type: e.type
                , key: externs.inKey(toSlice[keyEnc](e.key))
              }

              if (e.value !== undefined)
                o.value = externs.inValue(toSlice[valueEnc](e.value))

              return o
            }
            return {}
          })
        } else {
          arr = arr_
        }

        externs.batch(
            this
          , [ arr, options, callback ]
          , function (arr, options, callback) {
              this._db.batch(arr, options, function (err) {
                if (err) {
                  err = new errors.WriteError(err)
                  return dispatchError(err, callback)
                } else {
                  this.emit('batch', arr_)
                  if (callback)
                    callback()
                }
              }.bind(this))
            }
        )
      }

      LevelUP.prototype.approximateSize = function(start, end, callback) {
        var err

        if (!isOpening() && !isOpen()) {
          err = new errors.WriteError('Database is not open')
          return dispatchError(err, callback)
        }

        externs.approximateSize(
            this
          , [ start, end, callback ]
          , function (start, end, callback) {
              this._db.approximateSize(start, end, function(err, size) {
                if (err) {
                  err = new errors.OpenError(err)
                  return dispatchError(err, callback)
                } else if (callback)
                  callback(null, size)
              }.bind(this))
            }
        )
      }

      LevelUP.prototype.readStream =
      LevelUP.prototype.createReadStream = function (options) {
        options = extend(
            extend({}, this._options)
          , typeof options == 'object' ? options : {}
        )

        return externs.createReadStream(
            this
          , [ ReadStream, options ]
          , function (ReadStream, options) {
              return ReadStream.create(
                  options
                , this
                , externs
                , function (options) {
                    return this._db.iterator(options)
                  }.bind(this)
              )
            }
        )
      }

      LevelUP.prototype.keyStream =
      LevelUP.prototype.createKeyStream = function (options) {
        return this.readStream(extend(options, { keys: true, values: false }))
      }

      LevelUP.prototype.valueStream =
      LevelUP.prototype.createValueStream = function (options) {
        return this.readStream(extend(options, { keys: false, values: true }))
      }

      LevelUP.prototype.writeStream =
      LevelUP.prototype.createWriteStream = function (options) {
        return writeStream.create(extend(options), this)
      }

      LevelUP.prototype.toString = function () {
        return 'LevelUP'
      }

      levelup = new LevelUP(location, options)
      levelup.open(callback)
      return levelup
    }

module.exports      = createLevelUP
module.exports.copy = require('./util').copy
module.exports.use  = globalUse