
var queuer  = require('../queue')
var levelup = require('levelup')
var opts    = require('optimist').argv

var path = '/tmp/map-reduce-queue-test'

/**
So this isn't really a test...
it's just a script that I run, 
and then look at the output to see if it looks right.

run 

`node test/queue.js --crash`

to make the process crash, and then
when you start it again, it will also start those jobs.

Guess could run this as a child process a  few times,
and test that right number of jobs eventually complete.
**/

var TEST = 'test'
levelup(path, {createIfMissing: true}, function (err, db) {

  require('../use')(db)

  db.use(queuer({
    test: function (key, done) {
      console.log('START_WORK', key)
      setTimeout(function () {
        if(opts.crash && Math.random() < opts.crash) process.exit(1)
        console.log('DONE_WORK', key)
        done()
      }, 500)
    }
  }))

  db.once('queue:drain', function ready () {
    db.queue(TEST, 'hello')
    db.queue(TEST, 'bye')
    db.queue(TEST, 'hello')
    db.queue(TEST, 'hello')
  })
})
