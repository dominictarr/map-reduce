var EventEmitter = require('events').EventEmitter
var through = require('through')
var levelup = require('levelup')

function sk (ary) {
  if(!Array.isArray(ary))
    ary = [ary]

  //replace MAPR prefix with a view name?
  return '~MAPR~'+JSON.stringify(ary)
}

function liveQueue(work, delay, eventual) {
  var todo = {}
  delay = delay || 500
  return function queue(key) {

    var jsonKey = JSON.stringify(key)

    //TODO:
    //if the task has not run within EVENTUAL ms, just run it anyway.

    //delay each task until a key has stopped updating,
    //for DELAY ms.
    clearTimeout(todo[jsonKey])
    todo[jsonKey] = setTimeout(function () {
      work(JSON.parse(jsonKey))
    }, delay)
  }
}

module.exports = function (opts) {

  var emitter = new EventEmitter()
  var map = opts.map || function (key, value, emit) {emit(key, value)}
  var reduce = opts.reduce
  var db

  emitter.on('open', function (_db) {
    db = _db
    db.on('put', function (key, value) {
      console.log('PUT', key.toString(), value.toString())
      if(key < '~')
        doMap({key: key, value: value})
    })
    db.on('del', function (key) {
      //NOT IMPLEMENTED YET!
    })
  })

  var initial = opts.initial
  var queue = liveQueue(doReduce, 200)

  function group(key) {
    var start = key.slice()
    var end   = key.slice()
    start.push('')
    end.push('~')
    return {start: sk(start), end: sk(end)}
  }

  function doReduce (key) {
    if(!Array.isArray(key))
      key = JSON.parse(key)
    var collection = initial

    var values = []

    db.readStream(group(key))
      .pipe(through(function (data) {
        collection = reduce(collection, data.value, data.key)
      }, function () {
        //save the collection
        emitter.emit('reduce', key.slice(1),collection)

        key[0] = key[0] - 1
        //TODO: when queuing, write a queue message to the DB.
        //do it in a batch with the main update.
        //leveldb is optomized for batch updates, so this will be fast.
        db.put(sk(key), collection, function () {
          if(key[0] <= 0) return

          //queue the larger group to be reduced.
          key.pop()
          queue(key)
        })
      }))
  }

  function doMap (data) {
    var keys = [], sync = true, self = this
    //change the string key into a group key.
    function queueK (key, id) {
      if(!Array.isArray(key)) key = [key]

      key.unshift(key.length + 1)
      queue(key)
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
      db.batch(maps)
    })
  }

  //open the db
  levelup(opts.path, opts, function (err, _db) {
    db = _db
    if(err) return emitter.emit('error', err)
    emitter.emit('open', db)
  })

  emitter.force = function () {
    if(db) ready()
    else   emitter.once('open', ready)

    function ready () {

      var maps = {}

      //force the map-reduce to run.
      db.readStream(opts)
        .pipe(through(doMap))
    }

    return emitter
  }

  //read the results of a map-reduce
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

  return emitter
}
