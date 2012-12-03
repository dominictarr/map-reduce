var liveStream = require('level-live-stream')
var through = require('through')

module.exports = function (db, viewer) {
  liveStream(db)

  return function (opts) {
    opts = 'string' === typeof opts ? {name: opts} : opts
    var view = viewer.views[opts.name]
    var range = view.bucket.range(opts.start, opts.end)
    console.log(range)
    opts.start = range.start; opts.end = range.end

    var ls = opts.tail === false ? db.readStream(opts) : db.liveStream(opts)

    return ls.pipe(through(function (data) {
        var _data = {key: view.bucket.parse(data.key).key, value: data.value}
        this.queue(_data)
      })).once('close', ls.destroy.bind(ls))
  }
}
