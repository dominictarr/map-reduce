

var alphabet = 'abcdefghijklmnopqrstuvwxyz'

function randomLetter(n) {
  var a = alphabet[~~(Math.random()*26)]  
  return n ? a + randomLetter(n - 1) : a
}

var groups = []

function gen (a) {
  var l = 3
  a = a || []
  if(a.length > 3) return

  while(l --) {
    var _a = JSON.parse(JSON.stringify(a))
    _a.push(randomLetter(3))
    gen(_a)
    _a.unshift(_a.length)
    groups.push(JSON.stringify(_a))
  }
}

gen()

console.log(groups.sort())
