# Map Reduce for leveldb (via levelup)

Incremental map-reduces and real-time results.

## Waat?

An "incremental map reduce" means when you update one key,
only a relevant protion of the data needs to be recalculated.

"real-time results" means that you can listen to the database,
and recieve change notifications on the fly! a la 
[level-live-stream](https://github.com/dominictarr/level-live-stream)

## Example

### create a simple map-reduce

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
    reduce: function (acc, value, key) {
      //reduce little into big
      //must return a string or buffer.
      return return ''+(Number(acc) + Number(value))
    },
    //pass in the initial value for the reduce.
    //*must* be a string or buffer.
    initial: '0'

  })
})

```

`map-reduce` uses [level-hooks](https://github.com/dominictarr/level-hooks)
and [level-queue](https://github.com/dominictarr/level-queue) to make map reduces durable.

### querying results.

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

by default, the stream will stay open, and continue to give you the latest results.
This may be disabled by passing `{tail:false}`. 
The stream responds correctly to `stream.pause()` and `stream.resume()`

``` js
  db.mapReduce.view(viewName, {start: ['all', true], tail: false}) 
```

### complex aggregations

map-reduce with multiple levels of aggregation.

suppose we are building a database of all the street-food in the world.
the data looks like this:

``` js
{
  country: USA | Germany | Cambodia, etc...
  state:   CA | NY | '', etc...
  city: Oakland | New York | Berlin | Phnom Penh, etc...
  type: taco | chili-dog | doner | noodles, etc...
}
```

we will aggregate to counts per-region, that look like this:

``` js
//say: under the key USA
{
  'taco': 23497,
  'chili-dog': 5643,
  etc...
}
```

first we'll map the raw data to `([country, state, city, street],type)` tuples.
then we'll count up all the instances of a particular type in that region!

``` js

var levelup = require('levelup')
var mapReduce = require('map-reduce')

levelup(flie, {createIfMissing:true}, function (err, db) {

  mapReduce(db)

  db.mapReduce.add({
    name  : 'streetfood',
    map   : function (key, value, emit) {
      //perform some mapping.
      var obj = JSON.parse(value)
      //emit(key, value)
      //key may be an array of strings. 
      //value must be a string or buffer.
      emit(
        [obj.country, obj.state || '', obj.city],
        //notice that we are just returning a string.
        JSON.stringify(obj.type)
      )
    },
    reduce: function (acc, value) {
      acc = JSON.parse(acc)
      value = JSON.parse(value)
      //check if this is top level data, like 'taco' or 'noodle'
      if('string' === typeof value) {
        //increment by one (remember to set as a number if it was undefined)
        acc[value] = (acc[value] || 0) ++
        return JSON.stringify(acc)
      }
      //if we get to here, we are combining two aggregates.
      //say, all the cities in a state, or all the countries in the world.
      //value and acc will both be objects {taco: number, doner: number2, etc...}

      for(var type in value) {
        //add the counts for each type together...
        //remembering to check that it is set as a value...
        acc[type] = (acc[type] || 0) + value[type]
      }
      //stringify the object, so that it can be written to disk!
      return JSON.stringify(acc)
    },
    //pass in the initial value for the reduce.
    //*must* be a string or buffer.
    initial: '{}'
  })
})
```

then query it like this:

``` js
//pass tail: false, because new streetfood doesn't appear that often...
db.mapReduce.view('streetfood', {start: ['USA', 'CA'], tail: false})
  .pipe(...)
//or get the streetfood counts for each state. 
//we want to know about realtime changes this time.
db.mapReduce.view('streetfood', {start: ['USA', true]})

```



## License

MIT
