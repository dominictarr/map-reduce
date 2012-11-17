var test = require("tap").test
    , assert = require("assert")
    , toArray = require("write-stream").toArray

    , sum = require("./fixtures/sum")
    , MapReduce = require("..")

// test("sum", function (t) {
//     // Open a map reduce db on the levelup uri
//     var uri = "/tmp/map-reduce-sum-test-2"
//     var db = MapReduce(uri)

//     sum(uri, function (err) {
//         t.equal(err, null)

//         // Query the db to run the map reduce on a range of keys
//         var stream = db.query("sum", {
//             start: "0"
//             , end: "9"
//             , initial: 0
//         })

//         // wait for data to be reduced
//         stream.pipe(toArray(function (list) {
//             var value = list[0]

//             t.equal(value[0], "id")
//             t.equal(value[1], (1000 * 1001) / 2)
//             t.end()
//         }))
//     })

//     // Define a map reduce query
//     db.view("sum", function map(number, emit) {
//         emit("id", number)
//     }, function reduce(key, values) {
//         // values are strings. JSON stringified values
//         values = values.map(function (v) {
//             JSON.parse(v)
//         })

//         // Result should be an array of strings
//         return values.reduce(function (a, b) {
//             return a + b
//         }, 0).map(function (n) {
//             return JSON.stringify(n)
//         })
//     })
// })
