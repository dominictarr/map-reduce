var MapReduce = require('..')
var assert    = require('assert')
var through   = require('through')
var rimraf    = require('rimraf')
var levelup   = require('level-test')()
var SubLevel  = require('level-sublevel')

var db = SubLevel(levelup('map-reduce-deletes-test'))

var vowels = 'AEIOU'.split('')

var mapper = MapReduce(db, 'deletes',
  function (key, value, emit) {
    if(~vowels.indexOf(''+key))
      emit('vowel', value || 0)
    else
      emit('consonant', value || 0)
  },
  function (big, little, key) {
    return (Number(big || 0) + Number(little || 0)).toString()
  }, '0')

function times(n, delay, it, done) {
  var interval = setInterval(function () {
    it(n)
    if(--n <= 0) {
      clearInterval(interval); done && done()
    }
  }, delay)
}


var keys = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
var puts = [], deletes = []
var sum = 0

times(26, 100, function (i) {
  var k = keys[i - 1]
  puts.push(k)
  db.put(k, ''+i, function (err) {
    if(err) throw err
  })
}, function () {      
  times(26, 100, function (i) {
    var k = keys[i - 1]
    if(~vowels.indexOf(k)) {
      deletes.push(k)
      db.del(k)
    } else {
      sum += i
    }
  }, function () {
    console.log('SUM', sum)
  })
})

//eventually, this should emit [], 300
//and                          [], 10

var _group, _vowels, _consonant

mapper.on('reduce', function (key, sum) {
  console.log(key, sum)
  if(key.length == 0 && sum == 300)
    _group = true
  else if(key[0] == 'vowel' && sum == 0) 
    _vowels = true
  else if(key[0] == 'consonant' && sum == 300) 
    _consonant = true
})

process.on('exit', function () {
  assert(_group, 'group')
  assert(_vowels, 'vowels')
  assert(_consonant, 'consonant')
  console.log('passed')
})

/*
db.mapReduce.view('deletes', {start: [true]})
  .on('data', function (data) {
    console.log(''+data.key,''+data.value)
  })
*/

