
var MR      = require('..')
var sum     = require('./fixtures/sum')
var levelup = require('levelup')
var assert  = require('assert')
var through = require('through')
var mac     = require('macgyver')().autoValidate()
var pad     = require('pad')


sum('/tmp/map-reduce-sum-test-range', function (err, db) {
  if (err)
    throw err

  MR({
      start: '00000'
    , end: '00002'
    , name: 'range'
    , map: function (key, value) {
      console.log("v", value)
      this.emit("const", value)
    }
    , reduce: function (big, little, key) {
        return JSON.stringify(JSON.parse(big) + JSON.parse(little))
      }
    , initial: 0
  })(db)

  db.mapReduce.start('range')

  db.on('map-reduce:reduce:range', function (key, sum) {
    console.log('range', sum)
    // 0, 1, 2 and 10000 are the individuals. Then 10-19,
    // 100-199 and 1000-1999
    console.log('k',key, sum, (19 * 20) / 2)
    assert.equal(JSON.parse(sum), (19 * 20) / 2)
  })

  db.mapReduce.view('range', {start: [true]})
    .on('data', function (data) {
      console.log(''+data.value)
    })
})
