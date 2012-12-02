var hash = require('sha1sum')

module.exports = function (job, delay) {
  var jobs = []

  return function (value, done) {

    value = value
    var key = hash(value)

    function go() {
      delete jobs[key]
      job(value, done)
    }

    var old = jobs[key]
    if(old) clearTimeout(old.timeout)
    jobs[key] = {done: done, timeout: setTimeout(go, delay || 500)}
    //mark the old job as done.
    if(old && 'function' === typeof old.done) old.done()

  }

}
