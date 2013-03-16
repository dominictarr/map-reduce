
var MapReduce = require('..')
var SubLevel  = require('level-sublevel')
var sum       = require('./fixtures/sum')
var assert    = require('assert')
var through   = require('through')
var mac       = require('macgyver')().autoValidate()
var pad       = require('pad')

var dir = '/tmp/map-reduce-sum-test'
require('rimraf').sync(dir)

sum(dir, function (err, db) {
  if(err)
    throw err
  SubLevel(db)
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
    console.log('reduce', key, sum)
      sum = Number(sum)
      //this may callback with the wrong total before the process is finished.
      //need to assert that this value is EVENTUALLY equal to (1000 * 1001) / 2
      if(key.length == 0 && sum == ( 1000 * 1001 ) / 2)
        TOTAL = true
  }).atLeast(3))

  process.on('exit', function () {
    assert.ok(TOTAL, 'eventually hit the right value')
  })

//*/
  /*
  db.mapReduce.view('sum', [])
    .on('data', mac(function (data) {
      console.log('LIVE', data.key, ''+data.value)
    }).atLeast(1))
  */
})

