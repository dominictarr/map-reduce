
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
      if(key.length == 0) {
        assert.equal(JSON.parse(sum), ( 1000 * 1001 ) / 2)
    }
  }).times(3))

//*/
  /*
  db.mapReduce.view('sum', [])
    .on('data', mac(function (data) {
      console.log('LIVE', data.key, ''+data.value)
    }).atLeast(1))
  */
})

