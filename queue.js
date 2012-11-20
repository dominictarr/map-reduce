/**
LevelUp Queue.
**/

var through = require('through')

module.exports = function (prefix, work) {

return function (db) {

  var jobs = [], active = {}, I = 0, batch = []

  if('string' !== typeof prefix)
    prefix = '~QUEUE', work = prefix

  if(work && 'object' === typeof work) {
    for(var job in work)
      db.on('queue:start:'+job, work)
  }

  /**
  on start up, read any unfinished jobs from database 
  (incase there was a crash)

  cleanup any duplicates, 
  (possible if a delete fails and that job is requeued concurrently)

  then start the jobs.

  make i into a timestamp! a monotonic-timestamp
  **/

  var inProgress = 0
  var count = 0
  
  db.readStream({start: prefix , end: prefix+'~~'})
    .on('data', function (data) {
      count ++
      //KEY should be VALUE
      var value = ''+data.value
      var ary = (''+data.key).split('~')
      var i   = ary.pop()
      var job = ary.pop()

      db.emit('queue:recover', job, value)
      db.emit('queue:recover:'+job, value)
      start(job, i, value)
      I = Math.max(I, i)
    })
    .on('end', function () {
      //emit drain if there was no data.
      if(!count)
        db.emit('queue:drain')
    })

  function toKey(job, i) {
    var k = [prefix, job, i.toString()].join('~')
    return k
  }

  function start (job, i, value) {
    inProgress ++
    function done () {
      db.del(toKey(job, i), function () {
        inProgress --
        try {
          db.emit('queue:done', job, value)
          db.emit('queue:done:'+job, value)
        } finally {
          if(!inProgress)
            db.emit('queue:drain', key)
        }
      })
    }
    db.emit('queue:start', job, value, done)
    //you should probably just use this pattern...
    db.emit('queue:start:'+job, value, done)
  }

  /**
    pass array of keys + batch to associate jobs with a batch insert.
    this will mean that if the PUT can't be done without scheduling the jobs.

    (even if the process crashes immediately after)
  **/

  function queue (job, value, batch) {
    //XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    //CHANGING KEY -> VALUE
    var values = Array.isArray(value) ? value : [value] 
    batch = batch || []
    var toStart = []

    values.forEach(function (value) { 
      toStart.push({key: job, i: ++I, value: value})
      batch.push({type: 'put', key: toKey(job, I), value: value})
    })

    if(batch.length)
      db.batch(batch, function (err) {
        if(err) throw err //? is this the right thing to do here?
        toStart.forEach(function (a) {
          start(a.key, a.i, a.value)
        })
      })
  }

  db.queue = queue

}
}
