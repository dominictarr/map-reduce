
//patch middleware onto levelup.
//TODO merge this into levelup,
//but, let this idea/api grow a bit here first...
module.exports = function (db) {

  if(db.hook && db.use) return db

  //patch put, del, batch to say, add a patch that writes logs...
  //or whatever. will probably want the possibility to do an async hook,
  //so that it's possible to do write locks.

  //hook into put, get
  db.hook = function (hook) {
    //TODO
  }

  db.use = function (attach) {
    //this is all I'm doing right now, 
    //but might want to pass a `ready` cb,
    //so that each plugin can run in order...

    //leave that out until have something
    //I actually need it for, though...
    attach(db)
    return db
  }

  return db
}
