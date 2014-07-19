
var MapReduce = require('..')
var SubLevel  = require('level-sublevel')
var sum       = require('./fixtures/sum')
var assert    = require('assert')
var through   = require('through')
var mac       = require('macgyver')().autoValidate()
var pad       = require('pad')

var dir = 'map-reduce-sum-test'

var total = 0
var n = 1000, target = (n * (n+1))/2
sum(dir, n, function (err, db) {
  if(err)
    throw err
  var TOTAL = false

  var mapper = MapReduce(db, 'sum',
    function (key, value, emit) {
      value = JSON.parse(value)
      emit(value % 2 ? 'odd' : 'even', value || '0')
    },
    function (big, little) {
      return JSON.stringify(JSON.parse(big || 0) + JSON.parse(little))
    }).start()

  mapper.on('reduce',
    mac(function (key, sum) {
      total = sum = Number(sum)
      //this may callback with the wrong total before the process is finished.
      //need to assert that this value is EVENTUALLY equal to (n * (n + 1)) / 2
      console.log(key, sum)
      if(key.length == 0 && sum == target)
        TOTAL = true
  }).atLeast(3))

  process.on('exit', function () {
    assert.ok(TOTAL, 'eventually hit the right value: ' + target + ' but got to: ' + total)
  })
})

