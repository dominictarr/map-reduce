

var mapR = require('../map-reduce')
  , it = require('it-is').style('colour')

exports ['map'] = function (test){

    mapR({
      on: [1,2,3],
      map: function (emit,value){
        emit.next(value)
      },
      reduce: function (col,value){
        col = col || []
        col.push(value)
        return col
      },
      done: check
    })
  
  function check(err,results){
    it(results).deepEqual([1,2,3])

    test.done()
  }
}

exports ['map default to identity'] = function (test){

    mapR({
      on: [1,2,3],
      reduce: function (col,value){
        col = col || []
        col.push(value)
        return col
      },
      done: check
    })
  
  function check(err,results){
    it(results).deepEqual([1,2,3])

    test.done()
  }
}

exports ['reduce defaults to collect'] = function (test){

    mapR({
      on: [1,2,3],
      map: function (emit,value){
        emit.next(e*10)
      },
      done: check
    })
  
  function check(err,results){
    it(results).deepEqual([ 10, 20, 30 ])

    test.done()
  }
}

exports ['reduce defaults to collect'] = function (test){

    mapR({
      on: {a:1,b:2,c:3},
      map: function (emit,value,key){
        emit(value*10,key.toUpperCase()) 
        emit.next()
      },
      reduce: mapR.copy,
      done: check
    })
  
  function check(err,results){
    it(results).deepEqual({ A:10, B:20, C:30 })

    test.done()
  }
}

exports ['reduce stop early'] = function (test){

    mapR({
      on: {a:1,b:2,c:null},
      map: function (emit,value,key){
        if(!value)
          return emit.done('falsey')
        emit.next()
      },
      reduce: mapR.first,
      done: check
    })
  
  function check(err,results){
    it(results).strictEqual('falsey')

    test.done()
  }
}


exports ['max'] = function (test){

    mapR({
      on: [1,23,35,56,456,34534534,12,0,12,3,320],
      reduce: mapR.max,
      done: check
    })
  
  function check(err,results){
    it(results).strictEqual(34534534)

    test.done()
  }
}

exports ['min'] = function (test){

    mapR({
      on: [1,23,35,-56,456,34534534,12,0,12,3,320],
      reduce: mapR.min,
      done: check
    })
  
  function check(err,results){
    it(results).strictEqual(-56)

    test.done()
  }
}