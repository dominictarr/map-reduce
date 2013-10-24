var bytewise = require('bytewise')

module.exports = function (sep, term, exports) {
  exports = exports || {}

  exports.parse = function (key) {
    var array = bytewise.decode(new Buffer(key, 'hex')).split(sep)
    var l = +array.shift()
    if(l == 0)
      return []
    return array
  }

  exports.stringify = function (key) {
    if('string' === typeof key)
      key = [key]
    var l = key.length

    return bytewise.encode(l + sep + key
    .map(function (e) {
      if('number' === typeof e)
        return e.toString()
      return  e
    })
    .filter(function (e) {
      return 'string' === typeof e && e
    })
    .join(sep)).toString('hex')
  }

  exports.range = function (array) {
    return {
      min: exports.stringify(array) ,
      max: exports.stringify(array) + term,
    }
  }

  return exports
}

module.exports ('!', '~', module.exports)
