var level = require('level-test')()
var test  = require('tape')
var pull  = require('pull-stream')
var pl    = require('pull-level')
var range = require('../range')
var MapReduce = require('../')

var db = level()
var sublevel = require('level-sublevel')

function exactly (n) {
  return function (read) {
    return function (abort, cb) {
      if(0 <=--n) read(abort, cb)
      else cb(true)
    }
  }
}

test ('ranges', function (t) {
  var db = sublevel(level('map-reduce-ranges'))
  //numbers under between 1-100

  var mapDb = MapReduce(db, 'mapper', function (key, value, emit) {
    key = +key
//    console.log([key - key % 10, key % 10], key)
    emit([(key - key % 10) / 10, key % 10], key)
  }, function (acc, item) {
    return +(acc || 0) + +item
  })

//  mapDb.post(console.log.bind(null, '---?'))

 var sums = {
    0: 0

  }

  function checkRange(depth, expected, cb) {
    var r = []
    var _depth = depth
    while(_depth--)
      r.push(true)
    
    pull(
      pl.read(mapDb, {range: r, tail: true}),
      pull.through(console.log.bind(null, '>?>?>?>')),
      exactly(expected),
      pull.through(function (e) {
          var a = range.parse(e.key)
          t.equal(a.length, depth)
          console.log(a, depth)
          console.log(a)
      }),
      pull.drain(console.log.bind(null, '>>>'), cb)
    )
  }

  pull(
    pull.count(100),
    pull.map(function (e) {
      return {key: e, value: e, type: 'put'}
    }),
    pl.write(db, function () {

       console.log('written')

       var n = 3
       checkRange(3, 100, function (err) {
          if(err) throw err
          if(--n) return
          t.end()
       })
       checkRange(2, 10, function (err) {
          if(err) throw err
          if(--n) return
          t.end()
       })
       checkRange(1, 1, function (err) {
          if(err) throw err
          if(--n) return
          t.end()
       })
    })
  )
})
