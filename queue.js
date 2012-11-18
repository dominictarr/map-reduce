/**
LevelUp Queue.
**/

var through = require('through')

function has(ary, item) {
  return ~ary.indexOf(item)
}

function add (ary, item) {
  if(!has(ary, item))
    return ary.push(item), true
  return false
}

function rm (ary, item) {
  var i = ary.indexOf(item)
  if(!~i) return false
  ary.splice(i, 1)
  return true
}

module.exports = function (db, prefix, work, ready) {
  var jobs = [], active = {}, i = 0, batch = []
  /**
  on start up, read any unfinished jobs from database 
  (incase there was a crash)


  cleanup any duplicates, 
  (possible if a delete fails and that job is requeued concurrently)

  then start the jobs.
  **/

  db.readStream({start: prefix, end: prefix+'~', reverse: true})
    .pipe(through(function (data) {
      var key = data.value.toString()
      if(!add(jobs, key)) //this job is already active!
        batch.push({type: 'del', key: data.key})
      
      var j = data.key.toString().split('~').pop()
      if(!isNaN(j))
        i = active[key] = Math.max(i, j)
    }, function () {
      function startAll() {
        jobs.forEach(start)
      }
      //delete any duplicate jobs, and then start all jobs.
      if(batch.length) db.batch(batch, startAll)
      else                             startAll()

      ready(null, queue)
    }))

  function toKey(key) {
    var k = [prefix, key, active[key].toString()].join('~')
    return k
  }

  function start (key) {
    work(key, function () {
      rm(jobs, key)
      db.del(toKey(key))
      delete active[key]
    })
  }

  /**
  add way to insert jobs within a batch insert.
  important, because then jobs that relate to other PUTs
  will be created atomically. Either there will be a PUT and a job,
  or both will fail.

  hang on... if a job is running, and then the job comes in again,
  do you want to restart the job?

  maybe... that should be up to the user.
  emit an event to show that the job has occured again.

  do I want to limit a job to only running once? 
  or is it better to restart a job?

  hmm, I think that is more correct...
  **/

  function queue (key, batch) {
    var keys = Array.isArray(key) ? key : [key] 
    batch = batch || []
    var toStart = []

    keys.forEach(function (k) { 
      if(add(jobs, key)) {
        active[key] = ++i
        toStart.push(key)
        //if the process crashes while 
        batch.push({type: 'put', key: toKey(key), value: key})
      } else {
        console.log('active', key)
        //emitter.emit('job', key) //?
      }
    })

    if(batch.length)
      db.batch(batch, function (err) {
        if(err) throw err //?
        toStart.forEach(start)
      })
  }

  return queue
}
