
var queuer     = require('level-queue')
var Bucket     = require('range-bucket')
var hooks      = require('level-hooks')
var liveStream = require('level-live-stream')
var delayJob   = require('./delay-job')

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

    db.hooks.pre(function (batch) {
      var l = batch.length
      for(var i = 0; i < l; i++) {
        var key = ''+batch[i].key
        opts.forEach(function (view) {
          var name = view.name
          if(view.start <= key && key <= view.end /*&& batch[i].type === 'put'*/)
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
        //simplify by doing the maps jobs atomically,
        //as part of the hook.
        //OH, map is async, because a single map can point to many.
        //and, it has implicit deletes.
        //it's a fan-out map.
        //and then reduce is fan-in.

        //there are two distict modules here which are conflated.

        .add('map:'+name, function (job, done) {
          db.get(job, function (err, doc) {
            doMap({key: job, value: doc}, done)
          })
        })
        .add('reduce:'+name, delayJob(doReduce))

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
              //queue the parent group to be reduced.
              _key.pop()
              batch.push(db.queue('reduce:'+name, JSON.stringify(_key), false))
            }

            db.batch(batch, cb)
          })
      }

      view._doMap = doMap

      function doMap (data, cb) {
        var keys = [], sync = true, self = this
        var mapOldKeys = bucket('map', data.key)
        db.get(mapOldKeys, function (err, oldKeys) {
          oldKeys = oldKeys ? JSON.parse(oldKeys) : []
          var maps = []
          function emit (key, value) {
            if(!sync) throw new Error('emit called asynchronously')
            if(!~keys.indexOf(key)) {
              var _key = bucket([key, data.key])
              keys.push([key, data.key])
              maps.push({
                type: 'put',
                //also, queue the next reduce.
                key: _key,
                value: value
              })
              //add job to batch
              maps.push(
                db.queue('reduce:'+name, JSON.stringify([key]), false)
              )
            }
          }
          emit.emit = emit

          //don't do a map if this was a delete.
          if('undefined' !== typeof data.value)
            view.map.call(emit, data.key, data.value, emit)

          //setting this will make emit throw if it is called again later.
          sync = false

          oldKeys.forEach(function (kAry) {              
              maps.unshift({type: 'del', key: bucket(kAry)})
              kAry = kAry.slice(); kAry.pop()
              maps.push(
                db.queue('reduce:'+name, JSON.stringify(kAry), false)
              )
          })
          //save the maps.
          maps.push({
            type: keys.length ? 'put' : 'del', 
            key: mapOldKeys, 
            value: keys.length ? JSON.stringify(keys) : null 
          })

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
