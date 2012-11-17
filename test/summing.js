var test = require("tap").test
    , assert = require("assert")
    , toArray = require("write-stream").toArray

    , MapReduce = require("..")

test("sum", function (t) {
    // Open a map reduce db on the levelup uri
    var db = MapReduce("/tmp/sum")

    // Define a map reduce query
    db.view("sum", function map(number, emit) {
        emit("id", number)
    }, function reduce(key, values) {
        return values.reduce(function (a, b) {
            return a + b
        }, 0)
    })

    // Query the db to run the map reduce on a range of keys
    var stream = db.query("sum", {
        start: "0"
        , end: "9"
    })

    // wait for data to be reduced
    stream.pipe(toArray(function (list) {
        var value = list[0]

        t.equal(value[0], "id")
        t.equal(value[1], (1000 * 1001) / 2)
        t.end()
    }))
})
