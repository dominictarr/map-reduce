

var EventEmitter = require('events').EventEmitter
var through = require('through')
var levelup = require('levelup')

module.exports = function (opts) {

  var emitter = new EventEmitter()
  var reduce = opts.reduce
  var initial = opts.initial
  var collection = initial

  levelup(opts.path, {}, function (err, db) {

    db.readStream({
      'start': opts.start
      , 'end': opts.end
    })
      .pipe(through(function (data) {
        collection = reduce(collection || initial, data.value)
        this.queue(collection)
      }, function () {
        this.queue(null)
        emitter.emit('reduce', collection)
      }))

  })

  return emitter

}
