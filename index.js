

var levelup = require('levelup')
var through = require('through')
var from    = require('from')

//this will hold the reductions to do.

var queue = []

function map (data, emit) {
  var company = JSON.parse(data)
  var id = company.name

  company.contacts.forEach(function (contact) {
//    emit(contact.last+', '+contact.first, id)
//    emit([contact.last, contact.first], id)
    emit([contact.first, contact.last], JSON.stringify([id]))
  })
}

//merge sets.
function reduce (big, little) {
  function prep(ary) {
    var ary = JSON.parse(ary)
    if(!Array.isArray(ary))
      ary = [ary]
    return ary
  }
  var big     = prep(big)
  var little  = prep(little)
  little.forEach(function (item) {
    if(!~big.indexOf(item))
      big.push(item)
  })
  big.sort()
  return JSON.stringify(big)
}

levelup('/tmp/reducer-testdb', function (err, db) {

  var writer = db.writeStream()

  function sk (key) {
    if(!Array.isArray(key)) key = [key]
    key = key.length + '~' + JSON.stringify(key)
    return key
  }

  db.readStream({end: '~', start: ''})
  .pipe(through(function (data) {
    //get any previous mapping for this object,
    //and delete it's keys...
    var k = data.key
    var v = data.value
    var self = this

    db.get('~map~~'+k, function (err, oldMap) {
      var old = oldMap ? JSON.parse(oldMap) : []

      var maps = [], keys = []

      function emit (key, val) {
        key = sk(key)
        keys.push(key)
        maps.push({type: 'put', key: '~map~' + key + '~' + k, value: val})
      }

      map(data.value, emit)

      //okay, so now these all must be saved...
      //also, so we will be able to update them,
      //save them as map!KEY!ID
      //also, save map!!!ID -> the keys.

      var bulk = [
        //they keys this value was mapped to.
        {type: 'put', key:'~map~~'+k, value: JSON.stringify(keys.sort())}
      ]

      //if a key was in old, but now keys, delete that map.
      old.forEach(function (key) {
        if(!~keys.indexOf(key)) {
          bulk.push({type: 'del', key: '~map~'+key+'~'+k})
        }
      })
      
      maps.forEach(function (m) {
        if(!~queue.indexOf(m.key))
          queue.push(m.key)
        bulk.push(m)
      })

      db.batch(bulk)

      //okay... so how do I decide where to bundle the reduce?
      //what about, when a key is updated
      //rereduce the whole group?
      //by doing a range query on the ~map~KEY~ to ~map~KEY~~
      //and save that to ~reduce~KEY
      //this basically leaves it up to the user...
      //so, when you update a doc... rerun the reduce for a key.
      //and then rerun the reduce for the rest of the keys.
      //this could potentially be a very large number...
      //how to partition this?
      //another approch maybe... how to make merkle tree?
      //clearly map each doc to hash(key), hash(value)
      //then, keys are evenly distributed, so it's easy to group by.
      //hmm, maybe just group by the letters in the key?
      //so, we sort by 

      //[16, 256, all...]

      //so, basically, well add items to a reduce queue...
      //and then they will be reduced in batches.

      //so, each map result needs to get added to a reduce queue.
      //that should then run every so often, and also group the
      //updates so that they run in a good order.

//      console.log(queue)

      self.queue(data)
    })
  }, function () {
    this.queue(null)
  })).on('end', function () {
    console.log(queue.sort())
    console.log('END')

    //REDUCE.

    from(queue).pipe(through(function (key) {
      //parse the key, and get a range.
      var ary = JSON.parse(key.split('~')[3])
      console.log(ary)


      var start = '~map~'+sk([ary[0], ''])
      var end   = '~map~'+sk([ary[0], '~'])
      console.log('START', start, 'END', end)
      var collection = JSON.stringify([])
      db.readStream({start: start, end: end})
        .pipe(through(function (data) {
          collection = reduce(collection, data.value)
        }, function () {
          console.log('COLLECTION', ary[0], collection)
        }))
    }))    
  })
})
