module.exports = parsed

function parsed (fun) {
  return function () {
    var args =
      [].slice.call(arguments)
        .map(function (e) {
          return JSON.parse(e)
        })
      return JSON.stringify(fun.apply(this, args))
  }
}
