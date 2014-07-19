'use strict'
var levelup  = require('level-test')()
var rimraf   = require('rimraf')
var pad      = require('pad')
var sublevel = require('level-sublevel')

function genSum (path, n, cb) {
  console.log(path)
  rimraf.sync(path)
  var db = sublevel(levelup(path))

  var l = n || 1e3, i = 0, N = n, total = 0
  while(l--) {
    db.put(
      pad(6, ''+ ++i, '0'),
      JSON.stringify(i),
      next
    )
    total += i
  }
  function next () {
    if(--N) return
    if(cb) cb(null, db)
  }

  return db
}

if(!module.parent) {
  genSum('/tmp/map-reduce-sum-test')
}

module.exports = genSum

