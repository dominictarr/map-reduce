var EventEmitter = require('events').EventEmitter
var through = require('through')
var levelup = require('levelup')
var queuer  = require('./queue')
var Bucket  = require('./range-bucket')
var hooks   = require('./hooks')

module.exports = function (opts) {

  var bucket = Bucket('mapr')

  var views = {}

  function add(opts) {
    opts.name = opts.name || 'default'
    views[opts.name] = opts
    opts.start =
      (opts.range ? opts.range.start : opts.start) || ''
    opts.end =
      (opts.range ? opts.range.end   : opts.end  ) || '~'

    //probably, I don't want to make map optional
    opts.map || function (key, value, emit) {emit(key, value)}    
  }

  if(!Array.isArray(opts)) opts = [opts]

  opts.forEach(add)

  return function (db) {

    var emitter = db
  
    db.use(queuer())
      .use(hooks())
      .hooks.pre(function (batch) {
        var l = batch.length
        for(var i = 0; i < l; i++) {
          var key = ''+batch[i].key

          opts.forEach(function (view) {
            var name = view.name
            if(view.start <= key && key <= view.end && batch[i].type === 'put')
              batch.push(db.queue('map:'+name, key, false))
          })
        }
        return batch
      })

    opts.forEach(function (view) {
      var name = view.name

      db.queue
        .add('map:'+name, function (job, done) {
          db.get(job, function (err, doc) {
            doMap({key: job, value: doc}, done)
          })
        })
        .add('reduce:'+name, function (job, done) {
          job = JSON.parse(job)
          if('string' === typeof job)
            throw new Error(JSON.stringify(job))
          var jsonKey = JSON.stringify(job)

          function go() {
            delete reducers[jsonKey]
            doReduce(job, done)
          }
          var old = reducers[jsonKey]
          if(old) clearTimeout(old.timeout)
          reducers[jsonKey] = {done: done, timeout: setTimeout(go, 500)}
          //mark the old job as done.
          if(old && 'function' === typeof old.done) old.done()
        })

      var reducers = {}

      function doReduce (key, cb) {
        if(!Array.isArray(key))
          key = JSON.parse(key)

        var collection = view.initial
        var values = []

        key.push(true)

        db.readStream(bucket.range(key))
          .pipe(through(function (data) {
            collection = view.reduce(collection, data.value, data.key)
          }, function () {
            //save the collection

            //get the parent group
            var _key = key.slice(); _key.pop()

            db.emit('reduce', name, _key, collection)
            db.emit('reduce:'+name, _key, collection)
            //TODO: when queuing, write a queue message to the DB.
            //do it in a batch with the main update.
            //leveldb is optomized for batch updates, so this will be fast.
            var batch = [{type:'put', key: bucket(_key), value: collection}]
            if(_key.length) {
              //if(key[0] <= 0) return

              //queue the parent group to be reduced.
              _key.pop()
              batch.push(db.queue('reduce:'+name, JSON.stringify(_key), false))
            }

            db.batch(batch, cb)
        
          }))
      }

      view._doMap = doMap

      function doMap (data, cb) {
        var keys = [], sync = true, self = this

        db.get(bucket('map', data.key), function (err, oldKeys) {
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
                db.queue('reduce:'+name, JSON.stringify([key]), false)
              )
            }
          }
          emit.emit = emit
          view.map.call(emit, data.key, data.value, emit)
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
    })

    //force the map-reduce to run.
    db.startMapReduce = function (key) {
      //delete range first?
      opts.forEach(function (view) {
        db.readStream(views)
          .on('data', function (data) {
            var key = ''+data.key
          //.pipe(through(view._doMap))
            opts.forEach(function (view) {
              var name = view.name

              if(view.start <= key && key <= view.end)
                view._doMap(data)
            })
  
          })
      })
      return db
    }
  }
}
