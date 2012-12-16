# Map Reduce for leveldb (via levelup)

Incremental map-reduces and real-time results.

## Waat?

An "incremental map reduce" means when you update one key,
only a relevant protion of the data needs to be recalculated.

"real-time results" means that you can listen to the database,
and recieve change notifications on the fly! a la 
[level-live-stream](https://github.com/dominictarr/level-live-stream)

## Example

``` js
var levelup = require('levelup')
var mapReduce = require('map-reduce')

levelup(flie, {createIfMissing:true}, function (err, db) {

  mapReduce(db)

  db.mapReduce.add({
    name  : 'example',  //defaults to 'default'
    start : '',         //defaults to ''
    end   : '~',        //defaults to '~' 
                        //map-reduce uses ~ to prefix special data, 
                        //because ~ is the last ascii character.
    map   : function (key, value, emit) {
      //perform some mapping.
      var obj = JSON.parse(value)
      //emit(key, value)
      //key may be an array of strings. 
      //value must be a string or buffer.
      emit(['all', obj.group], ''+obj.lines.length)
    },
    reduce: function (big, little, key) {
      //reduce little into big
      //must return a string or buffer.
      return return ''+(Number(big) + Number(little))
    },
    //pass in the initial value for the reduce.
    //*must* be a string or buffer.
    initial: '0'

  //CURRENTLY: install the map-reduce plugin like this:
  //the future possibly db.use(mapReduce)...
  })
})

```

`map-reduce` uses [level-hooks](https://github.com/dominictarr/level-hooks)
and [level-queue](https://github.com/dominictarr/level-queue) to make map reduces durable.

## querying results.

``` js
  //get all the results in a specific group
  //start:[...] implies end:.. to be the end of that group.
  db.mapReduce.view(viewName, {start: ['all', group]}) 

  //get all the results in under a group.
  db.mapReduce.view(viewName, {start: ['all', true]}) 

  //get all the top level 
  db.mapReduce.view(viewName, {start: []}) 

  //get a range
  db.mapReduce.view(viewName, {start: ['all', group1], end: ['all', groupN]}) 

```

`db.mapReduce.view()` returns an instance of 
[level-live-stream](https://github.com/dominictarr/level-live-stream)

If you do not want a live-stream, pass `{tail:false}`

``` js
  db.mapReduce.view(viewName, {start: ['all', true], tail: false}) 
```

## License

MIT
