var test = require("tap").test
    , assert = require("assert")
    , MapReduce = require("..")

test("sum", function (t) {
    var db = MapReduce("./fixtures")

    db.view("sum", function map(number, emit) {
        emit("id", number)
    }, function reduce(key, values) {
        return values.reduce(function (a, b) {
            return a + b
        }, 0)
    })

    var stream = db.query("sum")
})
