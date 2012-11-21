
var bucket = require('../range-bucket')
var assert = require('assert')

var assert = require('assert')
var alphabet = 'AOEUIDHTNSQJKXBMWVZPYFGCRLpyfgcrlaoeuidhtnszvwmbxkjq7531902468_-'

function random(n) {
  var a = alphabet[~~(Math.random()*26)]
  return ( n ? a + random(n - 1) : a )
}

var b = bucket('BUCKET')

assert.equal(typeof b, 'function')
assert.equal(typeof b.range, 'function')

function within(range, key) {
  return range.start <= key && key < range.end
}

function times (n, iter) {
  while(n-- > 0) iter(n)
}

times(1000, function () {
  var b  = bucket(random(6))
  var _b = bucket(random(6))
  var K  = random(6)
  var k  = b (K)
  var _k = _b(K)

  assert( within(b.range(), k) )
  //keys for a different bucket *must* fall outside the range.
  assert( !within(_b.range(),  k) )
  assert(  within(_b.range(), _k) )
})

times(1000, function () {
  var b  = bucket(random(6))
  var big = random(4), little = random(4), _little = random(4)

  assert(within(b.range([big, true]), b([big, little])))
  assert(within(b.range([big, little]), b([big, little])))
  assert(within(b.range([true, true]), b([big, little])))

  assert(!within(b.range([big, little]), b([big, _little])))
  assert(within(b.range([big, true]), b([big, _little])))
  assert(within(b.range([true, true]), b([big, _little])))
})

