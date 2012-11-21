

//abstract out the patterns I'm using to group documents into levelup!

/**
patterns I am currently using:

 ~PREFIX~[2, group1, group2]
or
  ~PREFIX~KEY

so, if you get everything between

{start: '~PREFIX~', end: '~PREFIX~~'}

that will be everything in the bucket, note: never use '' or '~' as a key.

I'm using JSON arrays for reduce groups.
I make the first key the depth, so that you can request just the 

that works because " (which ends a group) sorts before most characters (except !)

if there was a " in the string, then the order will be different.
gonna put this into the WHO CARES basket.

~ is the highest sorting ascii character
_ is not suitable, since it's in the middle.
! is the first, which is also acceptable,

**/

var order = 
  '7531902468AOEUIDHTNSPYFGCRLQJKXBMWVZaoeuidhtnspyfgcrlqjkxbmwv'
    .split('').sort().join('')

function valid (name, key) {
  if(/~/.test(key))
    throw new Error(name +' may not include "~" character')
  return key
}

function join() {
  return '~' + [].join.call(arguments, '~')
}

module.exports = function (bucket) {
  var args = [].slice.call(arguments)
  //remove any '~'
  args.forEach(function (e) {
    valid('bucket', e)
  })

  var bucket = args.join('~')

  function fromArray (key) {
    if(!Array.isArray(key))
      return key
    if(key.length > order.length)
      throw new Error(
          'group key too deep:' 
        + JSON.stringify(key) 
        + ' max depth is:' 
        + order.length
      )
    var ary = []
    key.forEach(valid.bind(null, 'group-key'))
    var l = key.length
    for(var i=0; i < l && true !== key[i]; i++)
      ary.push(key[i])
    
    var r = JSON.stringify(ary)
    return order[l] + ',' + r.substring(1, r.length - 1)
  }

  function toKey (key) {
    if(!key) return join(bucket)

    var ary = [].map.call(arguments, function (key) {
      valid('key', key)
      return fromArray(key)
    })
    ary.unshift(bucket)

    return join.apply(null, ary)

    //return join(bucket, key)
  }

  toKey.range = function (start, end) {
    //this should accept groups also
    //the range for the whole bucket.

    valid('start', start)
    valid('end'  , end)

    return {
      start: start ? toKey(start) : toKey(), 
      end:   end   ? toKey(end)   : (
        Array.isArray(start) ? toKey(start)+'~' :toKey()+'~~'
      )
    }
  }

  toKey.within = function (range, key) {
    return range.start < key && key < range.end
  }

  return toKey
}
