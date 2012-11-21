

//this will be used by queue.

module.exports = function (listener) {

  return function (db) {
    if(db.addPostHook) {
      if(listener) db.addPostHook(listener)
      return     
    }

    db._posthooks = []
    db.addPostHook = function (posthook) {
      db._posthooks.push(posthook)
    }

    if(listener) db.addPostHook(listener)

    function each (e) {
      db._posthooks.forEach(function (h) {
        h(e)
      })
    }

    db.on('put', function (key, val) {
      each({type: 'put', key: key, value: val})
    })
    db.on('del', function (key, val) {
      each({type: 'del', key: key, value: val})
    })
    db.on('batch', function onBatch (ary) {
      ary.forEach(each)
    })
  }
}
