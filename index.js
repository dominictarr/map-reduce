
var queuer     = require('level-queue')
var Bucket     = require('range-bucket')
var hooks      = require('level-hooks')
var liveStream = require('level-live-stream')

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

    if(db.mapReduce) {
      return db
    }
    db.mapReduce = {views: {}}

    //install plugins
    queuer()(db)
    hooks ()(db)
    liveStream(db)

    //OH NO, THIS STILL DOESN'T SUPPORT DELETES PROPERLY.
    db.hooks.pre(function (batch) {
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
      var bucket = Bucket('mapr', name)
      db.mapReduce.views[name] = bucket
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
          .on('data', function (data) {
            collection = view.reduce(collection, data.value, data.key)
          })
          .on('end', function () {
            //save the collection

            //get the parent group
            var _key = key.slice(); _key.pop()

            db.emit('map-reduce:reduce', name, _key, collection)
            db.emit('map-reduce:reduce:'+name, _key, collection)
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
            /***
              encountered a problem with doing hooks this way,
              if I turn a batch into a put, then db won't emit the post event,
              and there will be no post hook.
            ***/

            if(batch.length > 1)
              db.batch(batch, cb)
            else
              db.put(batch[0].key, batch[0].value, cb)
        
          })
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
    //I could save a map-reduce-state record into the db
    //which if not present means it's necessary to rerun the full map-reduce.
    db.mapReduce.start = function (key) {
      //delete range first?
      opts.forEach(function (view) {
        db.readStream(views)
          .on('data', function (data) {
            var key = ''+data.key
            opts.forEach(function (view) {
              var name = view.name

              if(view.start <= key && key <= view.end)
                view._doMap(data)
            })
          })
      })
      return db
    }

    db.mapReduce.view = function (name, opts) {
      opts = opts || {}
      var range = db.mapReduce.views[name].range(opts.start, opts.end)
      if(opts.tail === false)
        return db.readStream(range)
      return db.liveStream(range)
    }
  }
}
