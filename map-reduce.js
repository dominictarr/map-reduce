/*

  if your looking for just one idiom to do all your async programming, surely this is it.

args
  collection [] or {}
  each --defaults to identity (item,emit,next)
  reduce --defaults to push onto array

  collect items in a map:  
    function (collection,value,key){
      collection = collection || []
      collection.push(value)
      return collection
    }
  copy and object:
    function (collection,key,value){
      collection = collection || {}
      collection[key] = value
      return collection
    }
*/


exports = module.exports = map 

function map(opts){

  var keys = Object.keys(opts.on)
  var finished = false
  var reduced = opts.init

  opts.map = opts.map || exports.identity
  opts.reduce = opts.reduce || exports.collect

  function emit (){
    [].unshift.call(arguments,reduced)
    reduced = opts.reduce.apply(null,arguments)
  }
  function error (err) {
    finished = true
    opts.done(err,reduced)
  }
  
  function safe (funx) {
    return function (){
      var args = [].slice.call(arguments)
      try {
        funx.apply(this, args)
      } catch (err) {
        error(err)
      }
    }
  }
  
  function next () {
    if(arguments.length)
      emit.apply(null, arguments)

    if(!keys.length && !finished)
      return done()
    var key = keys.shift()
    try {
      opts.map(emit, opts.on[key],key)
    } catch (err) {
      if(finished) throw err //we're in the final callback. don't catch the error
      error(err)
    }
  }
  
  function done () {
    if(arguments.length)
      emit.apply(null, arguments)
    finished = true;
    opts.done(null, reduced)
  }

  emit.next = next
  emit.done = done
  emit.error = error
  emit.safe = safe

  next()//start
}

/*
  helper functions
*/
exports.id =
exports.identity = 
  function (emit, value, key) {
    emit(value, key); emit.next()
  }

exports.collect = 
  function (collection, value, key){
    collection = collection || []
    collection .push(value)
    return collection 
  }

exports.copy = 
  function (collection, value, key){
    collection = collection || {}
    collection[key] = value
    return collection
  }

exports.first = 
  function (collection, value, key){
    return collection === undefined ? value : collection
  }

exports.min = 
  function (min, value){
    if(min === undefined)
      return value
    return min <= value ? min : value
  }

exports.max = 
  function (max, value){
    if(max === undefined)
      return value
    return max > value ? max : value
  }

exports.sum = 
  function (total, value){
    if(total === undefined)
      return value
    return total + value
  }
