
var mapR    = require('../map-reduce')
var sum     = require('./fixtures/sum')
var assert  = require('assert')
var through = require('through')
var mac     = require('macgyver')().autoValidate()
var pad     = require('pad')


sum('/tmp/map-reduce-sum-test', function (err, db) {
  if(err)
    throw err
  mapR(db)

  db.mapReduce.add({
    name: 'sum',
    map: function (key, value) {
      value = JSON.parse(value)
      this.emit(value % 2 ? 'odd' : 'even', value)
    },
    reduce: function (big, little, key) {
      return JSON.stringify(JSON.parse(big) + JSON.parse(little))
    },
    initial: 0
  })

  db.mapReduce.start('sum')

//  db.on('map-reduce:reduce:sum', 

  db.on('reduce:sum', 
    mac(function (key, sum) {
//    console.log("REDUCE", key, sum)
    if(key.length == 0) {
      assert.equal(JSON.parse(sum), ( 1000 * 1001 ) / 2)
//      console.log('passed')
    }
  }).times(3))

//*/

  db.mapReduce.view('sum', {start: [], tail: false})
    .on('data', mac(function (data) {
      console.log('LIVE', data.key, ''+data.value)
    }).atLeast(1))

})

