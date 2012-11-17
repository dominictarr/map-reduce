
var MR      = require('..')
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

var mr = MR({
  path: '/tmp/map-reduce-sum-test',
  map: function (key, value) {
    value = JSON.parse(value)
    this.emit(value % 2 ? 'odd' : 'even', JSON.stringify(value))
  },
  reduce: function (big, little, key) {
    return JSON.stringify(JSON.parse(big) + JSON.parse(little))
  },
  initial: 0
})

mr.on('reduce', function (key, sum) {
  console.log("REDUCE", key, sum)
  if(key.length == 0) {
    assert.equal(JSON.parse(sum), ( 1000 * 1001 ) / 2)
    console.log('passed')
  }

  //mr.readStream({group: ['even']})
    //.pipe(through(console.log))
})
