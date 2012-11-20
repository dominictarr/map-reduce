var EventEmitter = require('events').EventEmitter
var through = require('through')
var levelup = require('levelup')
var queuer  = require('./queue')

function sk (ary) {
  if(!Array.isArray(ary))
    ary = [ary]

  //replace MAPR prefix with a view name?
  return '~MAPR~'+JSON.stringify(ary)
}

function bufferToString(b) {
  return JSON.stringify(Buffer.isBuffer(b) ? b.toString() : b)
}


module.exports = function (opts) {

  return function (db) {

  var emitter = db
  var map = opts.map || function (key, value, emit) {emit(key, value)}
  var reduce = opts.reduce
  
  db.use(queuer({
    map: function (job, done) {
      job = JSON.parse(job)
      db.get(job, function (err, doc) {
        doMap({key: job, value: doc}, done)
      })
    },
    reduce: function (job, done) {
      job = JSON.parse(job)
      if('string' === typeof job)
        throw new Error(JSON.stringify(job))

      var jsonKey = JSON.stringify(job)
      function go() {
        delete reducers[jsonKey]
        doReduce(job, done)
      }
      rTimeout = null
      var old = reducers[jsonKey]
      if(old) clearTimeout(old.timeout)
      reducers[jsonKey] = {done: done, timeout: setTimeout(go, 500)}
      //mark the old job as done.
      if(old && 'function' === typeof old.done) old.done()
    }
  }))

  var queue = db.queue.bind(db)

  db.on('put', function (key, value) {
    if(key < '~')
      queue('map', bufferToString(key))
  })
  db.on('del', function (key) {
    //NOT IMPLEMENTED YET!
  })

  var initial = opts.initial
  var reducers = {}, rTimeout

  function group(key) {
    var start = key.slice()
    var end   = key.slice()
    start.push('')
    end.push('~')
    return {start: sk(start), end: sk(end)}
  }

  function doReduce (key, cb) {
    if(!Array.isArray(key))
      key = JSON.parse(key)
    var collection = initial

    var values = []

    db.readStream(group(key))
      .pipe(through(function (data) {
        collection = reduce(collection, data.value, data.key)
      }, function () {
        //save the collection
        db.emit('reduce', key.slice(1),collection)

        key[0] = key[0] - 1
        //TODO: when queuing, write a queue message to the DB.
        //do it in a batch with the main update.
        //leveldb is optomized for batch updates, so this will be fast.
        db.put(sk(key), collection, function (err) {
          if(key[0] <= 0) return

          //queue the larger group to be reduced.
          key.pop()
          queue('reduce', bufferToString(key))
          cb(err)
        })
      }))
  }

  function doMap (data, cb) {
    var keys = [], sync = true, self = this
    //change the string key into a group key.

    function queueK (key, id) {
      if(!Array.isArray(key)) key = [key]
      key.unshift(key.length + 1)

      queue('reduce', bufferToString(key.slice()))

      key.push(id.toString())
      return sk(key)
    }
    //store the doc key -> mapped keys,
    //so that if the doc is updated,
    //and emits different keys
    //then we can remove the old maps.
    db.get('~MAPKEYS~'+data.key, function (err, oldKeys) {
      oldKeys = oldKeys ? JSON.parse(oldKeys) : []
      var maps = []
      function emit (key, value) {
        if(!sync) throw new Error('emit called asynchronously')
        if(!~keys.indexOf(key)) {
          keys.push(key)
          maps.push({
            type: 'put',
            //also, queue the next reduce.
            key: queueK(key, data.key),
            value: value
          })
        }
      }
      emit.emit = emit
      map.call(emit, data.key, data.value, emit)
      //setting this will make emit throw if it is called again later.
      sync = false
      oldKeys.forEach(function (_key) {
        if(!~keys.indexOf(_key))
          map.unshift({type: 'del', key: _key})
      })
      //save the maps.
      db.batch(maps, cb)
    })
  }

  db.startMapReduce = function (key) {
  
    //force the map-reduce to run.
    db.readStream(opts)
      .pipe(through(doMap))  

    return db
  }

  db.viewStream = function (opts) {
    //get a stream from a map-reduce...
  }

  //read the results of a map-reduce
/*
  emitter.readStream = function (opts) {
    //if opts.group is an array, use it to set start, end

    //abstract this out

    if(Array.isArray(opts.group)) {
      opts.group.unshift(opts.group.length + 1)
      var _opts = group(opts.group)
      opts.start = _opts.start
      opts.end = _opts.end
    } else {
      opts.start = ''; opts.end = '~'
    }

    //abstract this out, and provide access to the other levelup streams.
    if(db)
      return db.readStream(opts)
    var t = through ()
    emitter.once('load', function (db) {
      db.readStream(opts).pipe(t)
    })
    return t
  }
*/

  }
}
