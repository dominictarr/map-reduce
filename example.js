
var db = require('level-sublevel')
  (require('levelup')('/tmp/map-reduce-example'))

var MapReduce = require('./')

var mapped = MapReduce(db, 'example', function (key, value, emit) {
  console.log('MAP', key, value, '->', 2 % Number(value) ? 'even' : 'odd')
  if(2 % Number(value))
    emit('even', Number(value))
  else
    emit('odd', Number(value))
}, function (acc, v) {
  return Number(acc || 0) + Number(v)
})

db.put('a', '1', console.log)
db.put('b', '2', console.log)
db.put('c', '3', console.log)
db.put('d', '4', console.log)
db.put('e', '5', function () {

  mapped.on('reduce', function (group, val) {
    console.log('Reduce', group, val)
  })

})

