var map = require('./map')
var reduce = require('./reduce')

module.exports = function (db) {

  if(!db) return function module.exports

  if(db.mapReduce) return

  map(db)
  reduce(db)

  db.mapReduce = {}
  db.mapReduce.add = function (view) {
    db.map.add(view)
    db.reduce.add(view)
  }

  db.mapReduce.start = function (name, done) {
    if(!name) {
      var started = 0
      for(var name in db.map.views) {
        started ++
        db.map.start(name, next)
      }
      function done() {
        if(!--started) done && done()
      }
    }
  }
  db.mapReduce.view = db.reduce.view
}
