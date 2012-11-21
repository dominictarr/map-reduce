var rimraf  = require('rimraf')
var levelup = require('levelup')

var use     = require('../use')
var prehook = require('../prehook')
var Bucket  = require('../range-bucket')

var assert  = require('assert')
var mac     = require('macgyver')().autoValidate()

var dir ='/tmp/map-reduce-prehook-test'

rimraf(dir, function () {
  levelup(dir, {createIfMissing: true}, function (err, db) {

    var SEQ = 0
    var bucket = Bucket('prehook')
    use(db)
    .use(prehook(mac(function (batch) {
      //iterate backwards so you can push without breaking stuff.
      for(var i = batch.length - 1; i >= 0; --i) {
        var ch = batch[i]
        var key = ch.key
        if(key < '~')
          batch.push({
            type: 'put', 
            key: new Buffer('~log~'+ ++SEQ),
            value: new Buffer(JSON.stringify({
              type: ch.type, 
              key: key.toString(), 
              time: Date.now()
            }))
          })
      }
      batch.push({type: 'put', key: new Buffer('~seq'), value: new Buffer(SEQ.toString())})
      return batch
    }).atLeast(1)))

    var n = 3

    var next = mac(function () {
      console.log('test', n)
      if(--n) return
      console.log('go', n)

      db.get('~seq', mac(function (err, val) {
        console.log(''+val)
        assert.equal(Number(''+val), 3)
        db.readStream({start: '~log~', end: '~log~~'})
          .on('data', function (data) {
            console.log(data.key.toString(), data.value.toString())
          })
      }).once())
    }).times(3)

    db.put('hello' , 'whatever' , next)
    db.put('hi'    , 'message'  , next)
    db.put('yoohoo', 'test 1, 2', next)
  })
})


