
var MR      = require('..')
var sum     = require('./fixtures/sum')
var levelup = require('levelup')
var assert  = require('assert')
var through = require('through')
var mac     = require('macgyver')().autoValidate()
var pad     = require('pad')


sum('/tmp/map-reduce-sum-test-range', function (err) {
  if (err)
    throw err

  var mr = MR({
      path: '/tmp/map-reduce-sum-test-range'
    , start: '00000'
    , end: '00002'
    , map: function (key, value) {
      console.log("v", value)
      this.emit("const", value)
    }
    , reduce: function (big, little, key) {
        return JSON.stringify(JSON.parse(big) + JSON.parse(little))
      }
    , initial: 0
  }).force()

  mr.on('reduce', function (key, sum) {
    console.log('range', sum)
    // 0, 1, 2 and 10000 are the individuals. Then 10-19,
    // 100-199 and 1000-1999
    assert.equal(JSON.parse(sum), (19 * 20) / 2)
  })
})
