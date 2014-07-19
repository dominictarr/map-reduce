var bytewise = require('bytewise')

module.exports = function (min, max, exports) {
  exports = exports || {}

  exports.parse = function (key) {
    var array = bytewise.decode(new Buffer(key, 'hex'))
    var l = array.shift()
    return array
  }

  exports.stringify = function (key) {
    if(!Array.isArray(key))
      key = [key]
    return bytewise.encode([key.length].concat(key)).toString('hex');
  }

  exports.range = function (array) {
    return {
      min: exports.stringify(array.map(function(v) {
        if (v === true) return min; else return v
      })),
      max: exports.stringify(array.map(function(v) {
        if (v === true) return max; else return v
      })),
    }
  }

  return exports
}

module.exports (null, undefined, module.exports)
