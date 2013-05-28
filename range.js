
module.exports = function (sep, term, exports) {
  exports = exports || {}

  exports.parse = function (key) {
    var array = key.split(sep)
    var l = +array.shift()
    if(l == 0)
      return []
    return array
  }

  exports.stringify = function (key) {
    if('string' === typeof key)
      key = [key]
    var l = key.length

    return l + sep + key.filter(function (e) {
      return 'string' === typeof e
    }).join(sep)

   // .map(function (e) { return e + sep } )
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
