# Map Reduce for nodejs #

use nosql style map reduce functions to iterate over data in node.

1 function:

    var mapR = require('map-reduce')
    
    mapR({
      on: collection to iterate over (can be an object or an Array)
      map: iteration function (optional, defaults to mapR.identity
      reduce: reduction function (optional, defaults to mapR.collect)
      init: initial value for collection arg in reduce step, default to undefined
      done: callback function
    })
    

##map (emit, value, key) ##

calling `emit(value[,key,etc...])` will send args to the reduce function.

map-reduce is async, 
call `emit.next()` to go to the next item.
or, `emit.done()` to stop processing more items.

if the values to be emitted are passed into `emit.next` or `emit.done` that is like calling `emit(args); emit.next()`, or `emit(args); emit.done()`

##reduce (collection, value[, key,etc...]) ##

reduce will be called with a `collection` which is either, 

  1. the return value of the previous reduce call
  2. the .init value you supplied to mapR
  3. undefined
  
the other arguments are the values you called emit with.