var map    = require('../map')
var reduce = require('../reduce')

var levelup = require('levelup')
var rimraf  = require('rimraf')
var assert  = require('assert')
var through = require('through')
var mac     = require('macgyver')().autoValidate()

var path = '/tmp/level-map-test'

rimraf(path, function () {
  levelup(path, {createIfMissing: true}, function (err, db) {

    map(db)
    reduce(db)

    db.map.add(function test (key, value, emit) {
      console.log('MAP', ''+key, ''+value)
      var n = Number(''+value)
      emit(['numbers', 'square'], Math.pow(n, 2))
      emit(['numbers', 'sqrt'], Math.sqrt(n))
    })

    db.reduce.add({
      name: 'test',
      depth: 1,
      reduce: function (acc, value, key) {
        console.log(acc, value, key)
        return Number(acc) + Number(value)
      },
      initial: 0
    })

    db.put('a', 1)
    db.put('b', 2)
    db.put('c', 3)

    db.once('queue:drain', function () {
      db.put('c', '6')
      db.del('a')
    })

    db.on('reduce:test', function (key, col) {
      console.log('REDUCE', key, col)
    })
  })
})

