var EventEmitter = require('events').EventEmitter
var through = require('through')
var levelup = require('levelup')
var queuer  = require('./queue')
var Bucket  = require('./range-bucket')
var prehook = require('./prehook')

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

  var bucket = Bucket('mapr')

  var range = {
    start: (opts.range ? opts.range.start : opts.start) || '',
    end  : (opts.range ? opts.range.end   : opts.end  ) || '~'
  }

  return function (db) {

  var emitter = db
  var map = opts.map || function (key, value, emit) {emit(key, value)}
  var reduce = opts.reduce
  
  db.use(queuer({
    map: function (job, done) {
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
  .use(prehook(function (batch) {
    var l = batch.length
    for(var i = 0; i < l; i++) {
      var key = ''+batch[i].key
      if(range.start <= key && key <= range.end && batch.type === 'put')
        batch.push(queue('map', key, false))
    }
    return batch
  }))

  var queue = db.queue.bind(db)

  //use prehook here!
  var initial = opts.initial
  var reducers = {}, rTimeout

  function doReduce (key, cb) {
    if(!Array.isArray(key))
      key = JSON.parse(key)

    var collection = initial
    var values = []

    key.push(true)

    db.readStream(bucket.range(key))
      .pipe(through(function (data) {
        collection = reduce(collection, data.value, data.key)
      }, function () {
        //save the collection

        //get the parent group
        var _key = key.slice(); _key.pop()

        db.emit('reduce', _key, collection)
        //TODO: when queuing, write a queue message to the DB.
        //do it in a batch with the main update.
        //leveldb is optomized for batch updates, so this will be fast.
        var batch = [{type:'put', key: bucket(_key), value: collection}]
        if(_key.length) {
          //if(key[0] <= 0) return

          //queue the parent group to be reduced.
          _key.pop()
          batch.push(queue('reduce', JSON.stringify(_key), false))
        }

        db.batch(batch, cb)
        
      }))
  }

  function doMap (data, cb) {
    var keys = [], sync = true, self = this

    db.get(bucket('map', /*map,*/ data.key), function (err, oldKeys) {
      oldKeys = oldKeys ? JSON.parse(oldKeys) : []
      var maps = []
      function emit (key, value) {
        if(!sync) throw new Error('emit called asynchronously')
        if(!~keys.indexOf(key)) {
          keys.push(key)
          maps.push({
            type: 'put',
            //also, queue the next reduce.
            key: bucket([key, data.key]),
            value: value
          })
          //add job to batch
          maps.push(
            queue('reduce', JSON.stringify([key]), false)
          )
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

  //force the map-reduce to run.
  db.startMapReduce = function (key) {

    //delete range first?
    db.readStream(range)
      .pipe(through(doMap))  

    return db
  }

  }
}
