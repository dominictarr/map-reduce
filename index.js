var Trigger = require('level-trigger')
var range   = require('./range')
var next =
  typeof setImmediate == 'undefined'
  ? process.nextTick
  :  setImmediate

next = setTimeout

module.exports = function (db, mapDb, map, reduce, initial) {
  if('string' === typeof mapDb) mapDb = db.sublevel(mapDb)

  //store the keys a value has been mapped to.
  var mapper = mapDb.sublevel('mapped')

  if(!map)
    throw new Error('expected a map function')

  //when record is inserted, pull out what it was mapped to last time.
  var maps = Trigger(db, mapDb.sublevel('maps'), function (id, done) {
    mapper.get(id, function (err, oldKeys) {
      oldKeys = oldKeys ? JSON.parse(oldKeys) : []
      var newKeys = []

      db.get(id, function (err, value) {
        var batch = [], async = false

        //don't map if it's delete, just delete the old maps
        if(value != null)
          map(id, value, function (key, value) {
            var array = 'string' === typeof key ? [key] : key || []
            if(true == async) return console.error('map must not emit async')
            if(value == null || key == null) return
            array.push(id)
            batch.push({key: range.stringify(array), value: value, type: 'put'})
            newKeys.push(range.stringify(array))
          })

        async = true

        oldKeys.forEach(function (k) {
          if(!~newKeys.indexOf(k)) batch.push({key: k, type: 'del'})
        })

        batch.push({
          key: id,
          value: JSON.stringify(newKeys),
          type: 'put',
          prefix: mapper
        })

        mapDb.batch(batch, done)
      })
    })
  })

  var reduces

  if(reduce)
    reduces = Trigger(mapDb, 'reduces', function (ch) {
      var a = range.parse(ch.key);
      if(!a.length) return
      a.pop()
      return JSON.stringify(a)
    },
    function (a, done) {
      var array = JSON.parse(a)
      var acc = initial
      mapDb.createReadStream(range.range(array.concat(true)))
        .on('data', function (e) {
          try {
            acc = reduce(acc, e.value)
          } catch (err) {
            console.error(err);
            return done(err)
          }
        })
        .on('end', function () {
          var batch
          mapDb.batch([batch = {
            key  : range.stringify(array),
            value: ''+acc,
            type : acc == null ? 'del' : 'put'
          }], function (err) {
            if(err) return done(err)

            mapDb.emit('reduce', array, acc)
            done()
          })
        })
    })

  mapDb.start = function () {
    maps.start()
    reduces && reduces.start()
    return mapDb
  }

  //patch streams so that they can handle ranges.

  var createReadStream = mapDb.createReadStream

  mapDb.createReadStream = function (opts) {
    opts = opts || {}
    if(opts.range) {
      var r = range.range(opts.range)
      opts.start = opts.min = r.min
      opts.end = opts.max  = r.max
    }
    return createReadStream.call(this, opts)
  }

  mapDb.createViewStream = function(opts) {
    var stream = this.createReadStream(opts)
    stream.on('data', function(d) {
      d.key = range.parse(d.key)
    })
    return stream
  }

  var oldGet = mapDb.get
  mapDb.get = function(key){
    if(Array.isArray(key)) key = range.stringify(key)
    return oldGet.apply(this, arguments)
  }

  return mapDb
}
