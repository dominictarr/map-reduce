var MR      = require('..')
var assert  = require('assert')
var through = require('through')
var rimraf  = require('rimraf')
var levelup = require('levelup')

var dir ='/tmp/map-reduce-deletes-test' 

rimraf(dir, function () {

  levelup(dir, {createIfMissing: true}, function (err, db) {

    var vowels = 'AEIOU'.split('')

    MR(db)
    db.mapReduce.add({
      name: 'deletes',
      map: function (key, value) {
        //console.log('map', ''+key,''+value)
        if(~vowels.indexOf(''+key))
          this.emit('vowel', value)
        else
          this.emit('consonant', value)
      },
      reduce: function (big, little, key) {
        //console.log('reduce', big.toString(), little.toString())
        return JSON.stringify(Number(''+big) + Number(''+little))
      },
      initial: 0
    })

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

    db.on('reduce:deletes', function (key, sum) {
      console.log("REDUCE", key, sum)
      
      if(key.length == 0 && sum == 300)
        _group = true
      else if(key[0] == 'vowel' && sum === 0) 
        _vowels = true
      else if(key[0] == 'consonant' && sum == 300) 
        _consonant = true
    })

    process.on('exit', function () {
      assert(_group)
      assert(_vowels)
      assert(_consonant)
      console.log('passed')
    })

    db.mapReduce.view('deletes', {start: [true]})
      .on('data', function (data) {
        console.log(''+data.key,''+data.value)
      })

  })
})
