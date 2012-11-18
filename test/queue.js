
var queue = require('../queue')
var levelup = require('levelup')

var path = '/tmp/map-reduce-queue-test'

levelup(path, {createIfMissing: true}, function (err, db) {
  queue(db, '~QUEUE', function (key, done) {
    setTimeout(function () {
      if(Math.random() < 0.3) process.exit(1)
      console.log('DONE', key)
      done()
    }, 500)
  }, function (_, add) {
    add('hello')
    add('bye')
    add('hello')
    add('hello')
  })
})
