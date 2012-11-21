/**
LevelUp Queue.
**/

var through   = require('through')
var timestamp = require('monotonic-timestamp')
var posthook  = require('./posthook')

module.exports = function (prefix, work) {

  return function (db) {

    var jobs = [], active = {}, batch = []

    if('string' !== typeof prefix)
      work = prefix, prefix = '~queue'

    if(work && 'object' === typeof work) {
      for(var job in work)
        db.on('queue:start:'+job, work[job])
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

    function emit(name, a, b) {
      if(!a)
        return db.emit('queue:' + name)
      db.emit('queue:'+name,    a, b)
      db.emit('queue:'+name+':'+a, b)
    }

    function onJob (data) {
      //KEY should be VALUE
      var value = ''+data.value
      var ary = (''+data.key).split('~')
      var t   = ary.pop()
      var job = ary.pop()
      emit('recover', job, value)
      start(job, t, value)
    }

    //read any jobs left from last run.
    db.readStream({start: prefix , end: prefix+'~~'})
      .on('data', function (data) {
        count=true; onJob(data)
      })
      .on('end', function () {
        //emit drain if there was no data.
        if(!count && !inProgress)
          db.emit('queue:drain')
      })

    //listen for new jobs.
    db.use(posthook(function (change) {
      if(change.type == 'put' && /^~queue/.test(change.key)) {
        console.log('POSTHOOK', change.key+'', change.value+'')
        onJob(change)
      }
    }))


    function toKey(job, ts) {
      return [prefix, job, (ts || timestamp()).toString()].join('~')
    }

    function start (job, ts, value) {
      inProgress ++
      function done () {
        db.del(toKey(job, ts), function () {
          inProgress --
          try {
            db.emit('queue:done', job, value)
            db.emit('queue:done:'+job, value)
          } finally {
            if(!inProgress)
              db.emit('queue:drain')
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

    function queue (job, value, put) {
      var ts = timestamp()
      var key = toKey(job, ts)

/*      if(put === false) {
        //return the job to be queued, to include it in a batch insert.
          return {
          type: 'put', 
          key: key, 
          value: value
        }
      } else {*/
        db.put(key, value)
      //}
    }

    db.queue = queue
  }
}
