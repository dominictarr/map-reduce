
var levelup = require('levelup')
var opts = require('optimist').argv
var through = require('through')

if(!module.parent) {
  console.log(opts)
  levelup(opts._[0], function (err, db) {
    if(err) throw err
    db.readStream(opts)
      .pipe(through(function (data) {
        this.queue(data.key + ' :\n  ' + data.value + '\n\n')
      }, function () {
        this.queue(null)
      }))
      .pipe(process.stdout, {end: false})

  })
  
}
