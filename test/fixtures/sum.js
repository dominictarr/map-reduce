
var levelup = require('levelup')
var rimraf  = require('rimraf')
var pad     = require('pad')

function genSum (path, n, cb) {
  rimraf(path, function () {
    levelup(path, {createIfMissing: true}, function (err, db) {

      var l = n || 1e3, i = 0
      var stream = db.writeStream(), total = 0
      while(l--) {
        stream.write({key: pad(6, ''+ ++i, '0'), value: JSON.stringify(i)})
        total += i
      }
      stream.end()
      if(cb) stream.on('close', function () {
        console.log('TOTAL', total)

        cb(null, db)
      })
    })
  })
}

if(!module.parent) {
  genSum('/tmp/map-reduce-sum-test')
}

module.exports = genSum

