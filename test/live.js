var MapReduce = require('..')
var SubLevel  = require('level-sublevel')
var assert    = require('assert')
var through   = require('through')
var rimraf    = require('rimraf')
var levelup   = require('level-test')()

var dir ='map-reduce-live-test' 

var db = SubLevel(levelup(dir))

var vowels = 'aeiou'.split('')

var mr = MapReduce(db, 'live',
  function (key, value, emit) {
    if(~vowels.indexOf(key.toString().toLowerCase()))
      emit('vowel', value)
    else
      emit('consonant', value)
  },
  function (big, little, key) {
    console.log(big, little)
    return (
        Number((big || 0).toString())
      + Number(little.toString())
      ).toString()
  })


db.put('A', '10')
db.put('B', '20')
db.put('C', '30')
db.put('D', '40')
db.put('E', '50')
var total, vowels, consonants

mr.on('reduce', function (key, sum) {
  console.log(key, sum)
  if(key.length == 0) {
      total = +sum
  } else if(key[0] == 'vowel') 
    vowels = +sum //60
  else if(key[0] == 'consonant') 
   consonants = +sum
})


process.on('exit', function () {
  assert.equal(total, 150)
  assert.equal(vowels, 60)
  assert.equal(consonants, 90)
  console.log('passed')
})
