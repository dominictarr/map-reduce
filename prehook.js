
module.exports = function (hook) {

  return function (db) {
    if(db.addPreHook) {
      if(hook) db.addPreHook(hook)
      return
    }

    var hooks = []

    db.addPreHook = function (hook) {
      hooks.push(hook)
      return db
    }

    if(hook) db.addPreHook(hook)

    var put = db._db.put
    var del = db._db.del
    var batch = db._db.batch

    function callHooks (ch, opts, cb) {
      for (var i in hooks) {
        ch = hooks[i](ch)
        if(!ch) return cb(new Error('vetoed'))
      }
      if(ch.length == 1) {
        var change = ch.shift()
        return change.type == 'put' 
          ? put.call(db._db, change.key, change.value, opts, cb) 
          : del.call(db._db, change.key, opts, cb)  
      }
      return batch.call(db._db, ch, opts, cb)
    }

    db._db.put = function (key, value, opts, cb ) {
      var batch = [{key: key, value: value, type: 'put'}]
      return callHooks(batch, opts, cb)
    }

    db._db.del = function (key, opts, cb) {
      var batch =[{key: key, type: 'del'}]
      return callHooks(batch, opts, cb)
    }

    db._db.batch = function (batch, opts, cb) {
      return callHooks(batch, opts, cb)
    }
  }
}
