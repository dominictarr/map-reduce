var map = require('level-map')
var reduce = require('level-reduce')

module.exports = function (db) {

  if(db.mapReduce) return

  map(db)
  reduce(db)

  db.mapReduce = {}
  db.mapReduce.add = function (view) {
    view.map = view.map || function (key, value, emit) {
      emit([], value)
    }
    db.map.add(view)
    if(view.reduce)
      db.reduce.add(view)
  }

  db.mapReduce.start = function (name, done) {
    if(!name) {
      var started = 0
      for(var name in db.map.views) {
        started ++
        db.map.start(name, next)
      }
      function next() {
        if(!--started) done && done()
      }
    }
    else
      db.map.start(name, done)
  }
  db.mapReduce.view = db.reduce.view
}
