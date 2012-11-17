
var levelup = require('levelup')
var rimraf  = require('rimraf')

if(!module.parent) {

  var dir = '/tmp/map-reduce-sum-test'

  rimraf(dir, function () {

    levelup(dir, {createIfMissing: true}, function (err, db) {

    var l = 10e3, i = 0
    var stream = db.writeStream()
      while(l--)
        stream.write({key: JSON.stringify(i++), value: JSON.stringify(i)})
    })

  })

}


