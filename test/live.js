var MapReduce = require('..')
var SubLevel  = require('level-sublevel')
var assert    = require('assert')
var through   = require('through')
var rimraf    = require('rimraf')
var levelup   = require('levelup')

var dir ='/tmp/map-reduce-live-test' 

rimraf.sync(dir)

var db = levelup(dir)
SubLevel(db)

var vowels = 'aeiou'.split('')

var mr = MapReduce(db, 'live',
  function (key, value, emit) {
    console.log('map', key.toString(), value)
    if(~vowels.indexOf(key.toString().toLowerCase()))
      emit('vowel', value)
    else
      emit('consonant', value)
  },
  function (big, little, key) {
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

mr.on('reduce', function (key, sum) {
  console.log("REDUCE", key, sum)
  if(key.length == 0) {
    assert.equal(Number(sum), 150)
    console.log('passed')
    //mr.readStream({group: ['even']})
      //.pipe(through(console.log))
  } else if(key[0] == 'vowel') 
    assert.equal(Number(sum), 60)
  else if(key[0] == 'consonant') 
    assert.equal(Number(sum), 90)    
})


