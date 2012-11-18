
var MR      = require('..')
var sum     = require('./fixtures/sum')
var levelup = require('levelup')
var assert  = require('assert')
var through = require('through')

function parsed (fun) {
  return function () {
    var args =
      [].slice.call(arguments)
        .map(function (e) {
          return 'function' === typeof e ? e : JSON.parse(e)
        })
      return JSON.stringify(fun.apply(this, args))
  }
}

sum('/tmp/map-reduce-sum-test', function (err) {
  if (err) {
      throw err
  }

  var mr = MR({
    path: '/tmp/map-reduce-sum-test',
    map: function (key, value) {
      //value = JSON.parse(value)
      this.emit(value % 2 ? 'odd' : 'even', value)
    },
    reduce: function (big, little, key) {
      return JSON.stringify(JSON.parse(big) + JSON.parse(little))
    },
    initial: 0
  }).force()

  mr.on('reduce', function (key, sum) {
    console.log("REDUCE", key, sum)
    if(key.length == 0) {
      assert.equal(JSON.parse(sum), ( 1000 * 1001 ) / 2)
      console.log('passed')

      //mr.readStream({group: ['even']})
        //.pipe(through(console.log))
    }
  })
})

sum('/tmp/map-reduce-sum-test-range', function (err) {
  if (err) {
      throw err
  }

  var mr = MR({
    path: '/tmp/map-reduce-sum-test-range',
    start: '0'
    , end: '2'
    , reduce: parsed(function (a, b) {
      return a + 1
    })
    , initial: 0
  })

  mr.on('reduce', function (sum) {
    console.log('range', sum)
    // 0, 1, 2 and 10000 are the individuals. Then 10-19,
    // 100-199 and 1000-1999
    assert.equal(JSON.parse(sum), 4 + 10 + 100 + 1000)
  })
})
