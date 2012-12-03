var trigger = require('level-trigger')
var liveStream = require('level-live-stream')

var Bucket  = require('range-bucket')
var map     = require('map-stream')
var through = require('through')

module.exports = function (db) {

  if(db.reduce) return

  trigger(db)
  liveStream(db)

  db.reduce = {}

  db.reduce.add = function (view) {
    view.bucket = Bucket('mapr', view.name)
    view.depth = (view.depth && view.depth > 0) ? view.depth : 0
    var range = view.bucket.range()

    function doReduce (key, cb) {
      var collection = view.initial, values = []

      if(!Array.isArray(key))
        key = JSON.parse(key)

      key.push(true)

      db.readStream(view.bucket.range(key))
        .on('data', function (data) {
          collection = view.reduce(collection, data.value, data.key)
        })
        .on('end', function () {
          //save the collection
          //get the parent group
          var _key = key.slice(); _key.pop()
          console.log('r', key, collection)
          db.put(view.bucket(_key), collection, cb)

          db.emit('reduce', view.name, _key, collection)
          db.emit('reduce:'+view.name, _key, collection)
        })
    }

    db.trigger.add({
      start: range.start,
      end  : range.end,
      map  : function (data) {
        var key = view.bucket.parse(data.key).key
        if(key.length <= view.depth) {
          console.log('bottom!', key)
          return
        }
        console.log('RMAP', key, data.key, range.within(key))
        key.pop()
        return JSON.stringify(key)
      }, 
      job  : function (job, done) {
        doReduce(JSON.parse(job), done)
      }
    })
  }
}
