
var mapR    = require('..')
var sum     = require('./fixtures/sum')
var assert  = require('assert')
var through = require('through')
var mac     = require('macgyver')().autoValidate()
var pad     = require('pad')


sum('/tmp/map-reduce-sum-test', function (err, db) {
  if(err)
    throw err

  mapR({
      name: 'sum',
      map: function (key, value) {
        //value = JSON.parse(value)
        this.emit(value % 2 ? 'odd' : 'even', value)
      },
      reduce: function (big, little, key) {
        return JSON.stringify(JSON.parse(big) + JSON.parse(little))
      },
      initial: 0
    })(db)

  db.startMapReduce('sum')

  db.on('reduce:sum', mac(function (key, sum) {
    console.log("REDUCE", key, sum)
    if(key.length == 0) {
      assert.equal(JSON.parse(sum), ( 1000 * 1001 ) / 2)
      console.log('passed')

      //mr.readStream({group: ['even']})
        //.pipe(through(console.log))
    }
  }).times(3))
})

