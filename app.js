var http = require('http'); // For serving a basic web page.
var mongoose = require("mongoose"); // The reason for this demo.
var absorb = require('absorb');
var unescapeJs = require('unescape-js');
var AWS = require('aws-sdk');
var readline = require('readline');

AWS.config.update({region: 'us-east-1'});
/*for security reasons I have replaced  keys with xxx */
const credentials = new AWS.Credentials("xxxxx", "xxxxxxxxxx");

AWS.config.update({
    credentials: credentials, // credentials required for local execution
    region: 'us-east-1'
});

var s3 = new AWS.S3();

//Get the S3 file from the S3, bucket
//Bucket is public
var params = {Bucket: 'zimplistic-test-assets', Key: 'data.csv'}

// Here we find an appropriate database to connect to, defaulting to localhost if we don't find one.
var uristring =
    process.env.MONGODB_URI ||
    'mongodb://localhost/actvity_logs';

// The http server will listen to an appropriate port, or default to port 5000.
var theport = process.env.PORT || 4000;

// Makes connection asynchronously.  Mongoose will queue up database
// operations and release them when the connection is complete.
mongoose.connect(uristring, {useNewUrlParser: true}, function (err, res) {
    if (err) {
        console.log('ERROR connecting to: ' + uristring + '. ' + err);
    } else {
        console.log('Succeeded connected to: ' + uristring);
    }
});

//Define Schema for activity log
var logSchema = new mongoose.Schema({
    object_id: {type: Number, min: 0},
    object_type: {type: String, trim: true},
    timestamp: {type: Number, min: 0},
    object_changes: {type: Object, maxlength: 1000}
});


// Compiles the schema into a model, opening (or creating, if nonexistent) the 'LogData' collection in the MongoDB database
var PLogs = mongoose.model('LogData', logSchema);

// Clear out old data
PLogs.remove({}, function (err) {
    if (err) {
        console.log('error deleting old data.');
    }
});


const rl = readline.createInterface({
    input: s3.getObject(params).createReadStream()
});

rl.on('line', function (line) {
    //var eachItem = line;
    var newitem = line.split(",\"");
    //if object_changes is defined
    if (typeof newitem[1] != 'undefined') {
        var firstPart = newitem[0].split(",");
        var object_changes = newitem[1].substring(0, newitem[1].length - 1); //clean object_changes data

        object_changes = unescapeJs(object_changes);

        // Creating one log row.
        var logRow = new PLogs({
            object_id: firstPart[0],
            object_type: firstPart[1],
            timestamp: firstPart[2],
            object_changes: object_changes
        });

        // Saving each row to the database.
        logRow.save(function (err) {
            if (err) console.log('Error on save!')
        });
    }

})
    .on('close', function () {
        console.log('All data saved in mongoDB');
    });


// In case the browser connects before the database is connected, the
// user will see this message.
var found = ['DB Connection not yet established.  Try again later.  Check the console output for error messages if this persists.'];

// Create a rudimentary http server. 
// This is effectively the main interaction loop for the application. 
// As new http requests arrive, the callback function gets invoked.
http.createServer(function (req, res) {
    res.writeHead(200, {'Content-Type': 'text/html'});
    createWebpage(req, res);
}).listen(theport);


function parseResponse(response) {
    var result = {};
    if (response.length <= 0) {
        return result;
    }

    if (response.length == 1) {
        return JSON.parse(response[0].object_changes);
    }

    for (var i = response.length - 1; i >= 0; i--) {
        var currentPart = response[i].object_changes;
        result = absorb(result, JSON.parse(currentPart));
    }
    return result;

}

function createWebpage(req, res) {
    // Let's find all the documents
    PLogs.find({}).exec(function (err, result) {
        if (!err) {
            res.write(html1 + html2 + result.length + html3 + html31);

            //First Query
            var query = PLogs.find({'object_id': 1, 'object_type': "Order"}).sort({timestamp: -1}); // sort in descending order of timestamp
            query.where('timestamp').lt(1484731920);
            query.exec(function (err, result) {
                if (!err) {
                    var resp = parseResponse(result);
                    res.write(html4 + JSON.stringify(resp, undefined, 2) + html5);

                    //Second Query
                    var secondQuery = PLogs.find({'object_id': 1, 'object_type': "Order"}).sort({timestamp: -1}); // // sort in descending order of timestamp
                    secondQuery.where('timestamp').lt(1484722542);

                    secondQuery.exec(function (err, result) {
                        if (!err) {
                            var resp = parseResponse(result);
                            res.write(html7 + JSON.stringify(resp, undefined, 2) + html8);

                            //Third Query
                            var thirdQuery = PLogs.find({'object_id': 1, 'object_type': "Order"}).sort({timestamp: -1}); // sort in descending order of timestamp
                            thirdQuery.where('timestamp').lt(1484731400);

                            thirdQuery.exec(function (err, result) {
                                if (!err) {
                                    var resp = parseResponse(result);
                                    res.end(html10 + JSON.stringify(resp, undefined, 2) + html11);

                                } else {
                                    res.end('Error in third query. ' + err)
                                }
                            });

                        } else {
                            res.end('Error in second query. ' + err)
                        }
                    });

                } else {
                    res.end('Error in first query. ' + err)
                }
            });


        } else {
            res.end('Error in all documents query. ' + err)
        }
        ;
    });
}


// The listener in http.createServer should still be active after these messages are emitted.
console.log('http server will be listening on port %d', theport);
console.log('CTRL+C to exit');


//  HTML content in pieces.
var html1 = '<title> Activity log app </title> \
  <head> \
  <style> body {color: #394a5f; font-family: sans-serif} </style> \
  </head> \
  <body> \
  <h1> Activity log app </h1>  \
  <h2> CSV file was parsed and stored data in MongoDB database </h2> ';
var html2 = '<i>';
var html3 = ' Rows. </i> <br\><br\>';
var html31 = '<h1>Query Responses:</h1>';
var html4 = '<h2>1- What\'s the state of Order Id=1 At timestamp=1484733173 ?</h2> ';
var html5 = '';
var html6 = ' documents. </i>  <br\>';
var html7 = '<h2>2- What\'s the state of Order Id=1 At timestamp=1484722542?</h2>';
var html8 = '';
var html9 = ' documents. </i> <br\>';
var html10 = '<h2>3- What\'s the state of Order Id=1 At timestamp=1484731400?</h2> ';
var html11 = '';