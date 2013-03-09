
module.exports = function (sep, term, exports) {
  exports = exports || {}

  exports.parse = function (key) {
    var array = key.split(sep)
    var l = array.shift()
    return array
  }

  exports.stringify = function (key) {
    if('string' === typeof key)
      key = [key]
    var l = key.length

    return l + sep + key.filter(function (e) {
      return 'string' === typeof e
    }).join(sep)
  }

  exports.range = function (array) {
    return {
      start: exports.stringify(array),
      end: exports.stringify(array) + term,
    }
  }

}

module.exports ('!', '~', module.exports)
