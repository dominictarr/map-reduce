var MR      = require('..')
var assert  = require('assert')
var through = require('through')
var rimraf  = require('rimraf')
var levelup = require('levelup')

var dir ='/tmp/map-reduce-deletes-test' 

rimraf(dir, function () {

  levelup(dir, {createIfMissing: true}, function (err, db) {

    var vowels = 'aeiou'.split('')

    MR({
      name: 'deletes',
      map: function (key, value) {
        //console.log('map', ''+key,''+value)
        if(~vowels.indexOf(key.toString().toLowerCase()))
          this.emit('vowel', value)
        else
          this.emit('consonant', value)
      },
      reduce: function (big, little, key) {
        //console.log('reduce', big.toString(), little.toString())
        return JSON.stringify(Number(''+big) + Number(''+little))
      },
      initial: 0
    })(db)

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
    var i = 0

    times(26, 100, function (i) {
      var k = keys[i - 1]
      puts.push(k)
      db.put(k, ''+i, function (err) {
        if(err) throw err
      })
    }, function () {      
      times(21, 100, function (i) {
        var k = keys[i + 4]
        deletes.push(k)
        db.del(k)
      })
    })

    //eventually, this should emit [], 300
    //and                          [], 10

    db.on('map-reduce:reduce:deletes', function (key, sum) {
      console.log("REDUCE", key, sum)
      return
      if(key.length == 0) {
        assert.equal(Number(sum), 300)
        console.log('passed')
        //mr.readStream({group: ['even']})
          //.pipe(through(console.log))
      } else if(key[0] == 'vowel') 
        assert.equal(Number(sum), 20)
      else if(key[0] == 'consonant') 
        assert.equal(Number(sum), 20)
    })
  })
})

