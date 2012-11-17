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
  var map = opts.map
  var reduce = opts.reduce
  var db

  emitter.on('load', function (_db) {
    db = _db
  })

  var initial = opts.initial

  emitter.readStream = function (opts) {
    //if opts.group is an array, use it to set start, end

    //abstract this out

    var start, end
    if(Array.isArray(opts.group)) {

      //okay, so this is one way to do it.
      //prehaps you just want the group? not the 

      start = opts.group.slice()
      start.unshift(start.length + 1)
      start.push('')
      start = sk(start)

      end = opts.group.slice()
      end.unshift(end.length + 1)
      end.push('~')
      end = sk(end)

    } else {
      start = ''; end = '~'
    }

    //abstract this out
    if(db)
      return db.readStream({start: start, end: end})
    var t = through ()
    emitter.once('load', function (db) {
      db.readStream({start: start, end: end}).pipe(t)
    })
    return t
  }

  levelup(opts.path, {}, function (err, db) {
    emitter.emit('load', db)
    
    var maps = {}
    var queue = liveQueue(doReduce, 200)

    function doReduce (key) {
      console.log('DO REDUCE', key)
      if(!Array.isArray(key))
        key = JSON.parse(key)
      var collection = initial
      var start = key.slice()
      var end   = key.slice()
      start.push('')
      end.push('~')

      var values = []
      db.readStream({start: sk(start), end: sk(end)})
        .pipe(through(function (data) {
          collection = reduce(collection, data.value, data.key)
        }, function () {
          //save the collection
          emitter.emit('reduce', key.slice(1),collection)

          key[0] = key[0] - 1
          db.put(sk(key), collection)
          if(key[0] <= 0) return

          key.pop()
          queue(key)

        }))
    }

    db.readStream({start: '', end: "~"})
      .pipe(through(function (data) {
        var keys = []
        var sync = true
        var self = this

        //need to replace the two queues with just one queue.
        //an async queue, where it delays the execution of the reduce,
        //until it's actually needed.

        function queueK (key, id) {
          if(!Array.isArray(key)) key = [key]
          
          key.unshift(key.length + 1)
          queue(key)
          key.push(id)
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
      }))
  })
  return emitter
}
