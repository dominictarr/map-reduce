var MR      = require('..')
var assert  = require('assert')
var through = require('through')
var rimraf  = require('rimraf')
var levelup = require('levelup')

var dir ='/tmp/map-reduce-live-test' 

rimraf(dir, function () {

  levelup(dir, {createIfMissing: true}, function (err, db) {

    var vowels = 'aeiou'.split('')

    MR({
      name: 'live',
      map: function (key, value) {
        console.log('map', key.toString(), value)
        if(~vowels.indexOf(key.toString().toLowerCase()))
          this.emit('vowel', value)
        else
          this.emit('consonant', value)
      },
      reduce: function (big, little, key) {
        console.log(big.toString(), little.toString())
        return JSON.stringify(JSON.parse(big.toString()) + JSON.parse(little.toString()))
      },
      initial: 0
    })(db)


    db.put('A', '10')
    db.put('B', '20')
    db.put('C', '30')
    db.put('D', '40')
    db.put('E', '50')

    db.on('reduce:live', function (key, sum) {
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
  })
})

