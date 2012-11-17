var test = require("tap").test
    , assert = require("assert")
    , MapReduce = require("..")

test("sum", function (t) {
    var db = MapReduce("./fixtures")

    db.view("")
})
