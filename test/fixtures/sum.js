
var levelup = require('levelup')

if(!module.parent) {

  levelup('/tmp/map-reduce-sum-test', {createIfMissing: true}, function (err, db) {

  var l = 10e3, i = 1
  var stream = db.writeStream()
    while(l--)
      stream.write({key: JSON.stringify(i++), value: JSON.stringify(i)})
  })

}


