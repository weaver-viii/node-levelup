/* Copyright (c) 2012-2014 LevelUP contributors
 * See list at <https://github.com/rvagg/node-levelup#contributing>
 * MIT License <https://github.com/rvagg/node-levelup/blob/master/LICENSE.md>
 */

// NOTE: we are fixed to readable-stream@1.0.x for now
// for pure Streams2 across Node versions
var EncodingError  = require('level-errors').EncodingError
  , IteratorStream = require('level-iterator-stream')
  , through        = require('through2')
  , util           = require('./util')

module.exports = function (iterator, options, makeData) {
  var source = IteratorStream(iterator, options)
    , tr = through.obj(options, function (kv, _, done) {
        try {
          var value = makeData(kv.key, kv.value)
        } catch (e) {
          tr.emit('error', new EncodingError(e))
        }
        done(null, value)
      })
  source.pipe(tr)
  source.on('error', tr.emit.bind(tr, 'error'))
  source.on('close', tr.emit.bind(tr, 'close'))
  tr.destroy = source.destroy.bind(source)
  return tr
}
