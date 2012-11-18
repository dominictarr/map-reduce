/**
LevelUp Queue.
**/

var through = require('through')

module.exports = function (db, prefix, work, ready) {
  var jobs = [], active = {}, I = 0, batch = []

  if(work)
    db.on('queue:start', work)

  /**
  on start up, read any unfinished jobs from database 
  (incase there was a crash)

  cleanup any duplicates, 
  (possible if a delete fails and that job is requeued concurrently)

  then start the jobs.
  **/

  db.readStream({start: prefix , end: prefix+'~~'})
    .pipe(through(function (data) {
      var key = ''+data.value
      var i   = (''+data.key).split('~').pop()
      db.emit('queue:recover', key)
      start(key, i)
      I = Math.max(I, i)
    }, function () {
      db.queue = queue
      db.emit('queue:ready', queue)
      if(ready) ready(queue)      
    }))

  function toKey(key, i) {
    var k = [prefix, key, i.toString()].join('~')
    return k
  }

  function start (key, i) {
    db.emit('queue:start', key, function () {
      db.del(toKey(key, i), function () {
        db.emit('queue:done', key)
      })
    })
  }

  /**
    pass array of keys + batch to associate jobs with a batch insert.
    this will mean that if the PUT can't be done without scheduling the jobs.

    (even if the process crashes immediately after)
  **/

  function queue (key, batch) {
    var keys = Array.isArray(key) ? key : [key] 
    batch = batch || []
    var toStart = []

    keys.forEach(function (key) { 
      toStart.push({key: key, i: ++I})
      batch.push({type: 'put', key: toKey(key, I), value: key})
    })

    if(batch.length)
      db.batch(batch, function (err) {
        if(err) throw err //? is this the right thing to do here?
        toStart.forEach(function (a) {
          start(a.key, a.i)
        })
      })
  }

  return db
}
