var WriteStream = require("write-stream")
var assert = require("assert")
var MR = require("../incremental")
var insert = require("./fixtures/insert")
var parsed = require("../utils/parsed")

var uri = "/tmp/map-reduce-changes-test"
insert(uri, createTen(), function (err) {
    if (err) {
        throw err
    }

    var stream = MR({
        path: uri
        , map: function (value, emit) {
            emit(JSON.parse(value) % 2 ? "odd" : "even", value)
        }
        , reduce: parsed(function (a, b) {
            return a + b
        })
        , initial: 0
    })

    var evenSeen = false
        , oddSeen = false

    stream.pipe(WriteStream(function (chunk) {
        var key = chunk.key
            , value = JSON.parse(chunk.value)

        if (key === "even" && evenSeen === false) {
            assert.equal(value, 20)
            evenSeen = true
        } else if (key === "odd" && oddSeen === false) {
            assert.equal(value, 25)
            oddSeen = true
        } else if ()
    }))
})

function createTen() {
    var list = []
    for (var i = 0; i < 10; i++) {
        list.push({ key: "" + i, value: "" + i })
    }
    return list
}
