var trigger = require('level-trigger')
var liveStream = require('level-live-stream')

var Bucket  = require('range-bucket')
var map     = require('map-stream')
var through = require('through')

module.exports = function (db) {

  if(db.map) return

  trigger(db)
  liveStream(db)

  var views = {}
  db.map = {views: views}
  db.map.add = function (view) {
    var name = view.name
    if('function' === typeof view)
      view = {
        map: view, name: name, start: '', end: '~'
      }
    if('function' !== typeof view.map) throw new Error('expected map function')
    views[name] = view
    view.bucket = Bucket('mapr', name)
    
    db.trigger.add({
      start: view.start,
      end  : view.end,
      job  : function (key, done) {
        db.get(key, function (err, value) {
          doMap(view, {key: key, value: value}, done)
        })
      }
    })

    db.map.start = function (name, done) {
      //...
      db.readStream(views[name])
        .pipe(map(function (data, next) {
          doMap(views[name], data, next)  
        }))
        .on('end', done)
    }
    db.map.view = function (name, opts) {
      opts = 'object' === typeof name ? name : opts || {name: name}
      name = opts.name || name
      //...
      var r = views[name].bucket.range(opts.start, opts.end)
      opts.start = r.start
      opts.end   = r.end
      var ls = db.liveStream(opts)
      return ls.pipe(through(function (data) {
          var _data = {key: view.bucket.parse(data.key).key, value: data.value}
          console.log('view', _data)
          this.queue(_data)
        })).once('close', ls.destroy.bind(ls))
    }
  }

  function doMap (view, data, done) {
    var keys = [], sync = true, self = this, batch = []

    var kBucket = Bucket('mapr-keys', view.name)

    function emit (key, value) {
      if(!sync) throw new Error('emit called asynchronously')
      var _key = view.bucket([].concat(key).concat(data.key))
      batch.push({
        type: 'put',
        //also, queue the next reduce.
        key: _key,
        value: value
      })
      keys.push(_key)
    }

    emit.emit = emit
    //don't do a map if this was a delete.
    //will still delete the old mappings,
    //which will trigger a reduce (or whatevs)
    if('undefined' !== typeof data.value)
      view.map.call(emit, data.key, data.value, emit)
    //setting this will make emit throw if it is called again later.
    sync = false

    var mapOldKeys = kBucket(data.key)
    db.get(mapOldKeys, function (err, oldKeys) {
      oldKeys = (oldKeys ? JSON.parse(oldKeys) : [])
      
      //delete the old keys that arn't being updated.
      oldKeys.forEach(function (oldKey) {
        if(!~keys.indexOf(oldKey))
          batch.push({type: 'del', key: oldKey})
      })
      
      //save the maps.
      batch.push({
        type: keys.length ? 'put' : 'del', 
        key: mapOldKeys, 
        value: keys.length ? JSON.stringify(keys) : null 
      })

      db.batch(batch, done)
    })
  }
}

