"use strict";
let express = require('express');
let crud = require('express').Router();  // create a new route
let pg = require('pg'); // the code to connect to PostgreSQL
let fs = require('fs');  // code to read the database connection details file
// we need to get the user's name from the login used 
// to connect to the server
// that way we can work out the correct path for the connection file
//  /code/<<username>>/certs/


const os = require('os');
const username = os.userInfo().username;    // locate the database login details
console.log('\ncrud24 Str\n');
console.log(os.userInfo());
console.log(username);

// Load the database login details file
let configtext = "" + fs.readFileSync("certs/postGISConnection.js");// locate the database login details
// now convert the configruation file into the correct format -i.e. a name/value pair array
// this means looping through the file looking for commas
// each comma indicates a new line, a new piece of information
// we then take the information and convert it into a configuration
// for the PostgreSQL connection
let configarray = configtext.split(",");
let config = {};
for (let i = 0; i < configarray.length; i++) {
    let split = configarray[i].split(':'); //split = split one text string into two 
    config[split[0].trim()] = split[1].trim(); //trim = remove any spaces before or after the text
}
let pool = new pg.Pool(config);
console.log('\nconfig\n');
console.log(config);
console.log('\ncrud24 Fin\n');

let categoryToTableMap = {
    'Ceilings': 'bim_vis.office_ceiling',
    'SpecialtyEquipment': 'bim_vis.office_computers',
    'Doors': 'bim_vis.office_doors',
    'Exterior': 'bim_vis.office_exterior',
    'Floors': 'bim_vis.office_floors',
    'Furniture': 'bim_vis.office_furnitures',
    'Rooms': 'bim_vis.office_rooms',
    'Walls': 'bim_vis.office_walls',
    'Windows': 'bim_vis.office_windows'
};

// // TestCRUD end point (Using route)
// crud.route('/testCRUD').get(function (req, res) {
//     res.json({ message: req.originalUrl });
// });



const bodyParser = require('body-parser');
crud.use(bodyParser.urlencoded({ extended: true }));

// test endpoint for GET requests (can be called from a browser URL)
crud.get('/testCRUD', function (req, res) {
    res.json({ message: req.originalUrl + " " + "GET REQUEST" });
});

crud.get('/userId',function (req, res) {
    let querystring ='select user_id from cege0043.cege0043_users where user_name = current_user';

    pool.connect(function (err, client, done) {
        if (err) {
            console.log("not able to get connection " + err);
            res.status(500).send(err);
        }

        client.query(querystring, function (err, result) {
            done();
            if (err) {
                console.log(err);
                res.status(500).send(err);
            }
            else {
                console.log(result.rows[0].user_id);
                res.status(200).send(result.rows[0]);
            }
        });

    });
});

crud.post('/insertTestFormData', function (req, res) {
    let longitude = req.body.longitude;
    let latitude = req.body.latitude;
    let name = req.body.name;
    let surname = req.body.surname;
    let classModule = req.body.module;  // this variable is called classModule as module is a reserved word in JavaScript
    let language = req.body.language;
    let modulelist = req.body.modulelist;
    let lecturetime = req.body.lecturetime;

    let geometryString = "st_geomfromtext('POINT(" + longitude + " " + latitude + ")',4326)";

    let querystring = "INSERT into cege0043.formdata(name,surname,module,language, modulelist,lecturetime, location) values ";
    querystring += "($1,$2,$3,$4,$5,$6,";
    querystring += geometryString + ")";

    pool.connect(function (err, client, done) {
        if (err) {
            console.log("not able to get connection " + err);
            res.status(500).send(err);
        }
        client.query(querystring, [name, surname, classModule, language, modulelist, lecturetime], function (err, result) {
            done();
            if (err) {
                console.log(err);
                res.status(500).send(err);
            }
            else {
                res.status(200).send("Form Data " + req.body.name + " has been inserted");
            }
        });

    });

});

// Insert data to Petrol Station
crud.post('/insertPetrolStation', function (req, res) {
    let longitude = req.body.longitude;
    let latitude = req.body.latitude;
    let petrol_station_name = req.body.petrol_station_name;
    let last_inspected = req.body.last_inspected;
    let user_id = req.body.user_id;

    // var geometrystring = "st_geomfromtext('POINT(" + req.body.longitude + " " + req.body.latitude + ")',4326)";
    var geometrystring = "st_geomfromtext('POINT(" + longitude + " " + latitude + ")',4326)";

    var querystring = "INSERT into cege0043.petrol_station (petrol_station_name,last_inspected, location, user_id) values ";
    querystring += "($1,$2, ";
    querystring += geometrystring + ",$3)";

    pool.connect(function (err, client, done) {
        if (err) {
            console.log("not able to get connection " + err);
            res.status(500).send(err);
        }
        client.query(querystring, [petrol_station_name, last_inspected, user_id], function (err, result) {
            done();
            if (err) {
                console.log(err);
                res.status(500).send("You input a incorrect form with error: " + err);
            }
            else {
                res.status(200).send("Form Data " + req.body.petrol_station_name + " has been inserted");
            }
        });

    });

});

// Insert data to Price Queue
crud.post('/insertPriceQueueReport', function (req, res) {

    let petrol_station_name = req.body.petrol_station_name;
    let price_in_pounds = parseFloat(req.body.price_in_pounds);
    let queueLength = req.body.queue_length_id;
    let user_id = req.body.user_id;


    let querystring = "insert into cege0043.petrol_price_queue_information (petrol_station_id, price_in_pounds, queue_length_id,user_id) values (";
    querystring += "(select petrol_station_id from cege0043.petrol_station where petrol_station_name = $1),$2, (select queue_length_id from cege0043.queue_length where queue_length_description = $3),$4)";
    
    pool.connect(function (err, client, done) {
        if (err) {
            console.log("not able to get connection " + err);
            res.status(500).send(err);
        }

        client.query(querystring, [petrol_station_name, price_in_pounds, queueLength, user_id], function (err, result) {
            done();
            if (err) {
                console.log(err);
                res.status(500).send(err);
            }
            else {
                res.status(200).send("Form Data " + petrol_station_name +user_id+ " Price and Queue data" + " has been inserted");
            }
        });

    });

});

// http://127.0.0.1:4480/crud24/postgistestBIM
crud.get('/postgisTestBim', function (req, res) {

    // create a new connection in the pool
    // the connection will return err if it failes
    // if it works, the connection will return a client called client - which can be 
    // used to run some SQL 
    // done is the name of the function to be called once the SQL has
    // returned a value - this closes the connection so that it can be
    // reused
    pool.connect(function (err, client, done) {
        if (err) {
            console.log("not able to get connection " + err);
            res.status(400).send(err);
        }
        // the SQL that we want to run
        let query = "SELECT * FROM bim_vis.asset_suggestions";

        // pass the SQL to the client from the pool
        // will return err if it fails
        // result will hold any values returned by the SQL
        client.query(query, function (err, result) {
            done();
            if (err) {
                console.log(err);
                res.status(400).send(err);
            }
            else {
                // send to send the result back to the browser
                // result.rows will give an array of all the rows
                // in the result
                res.status(200).send(result.rows);
            }
        });
    });
});

// Insert data to Bim Form
crud.post('/insertBimSuggestions', function (req, res) {
    let objectId = req.body.objectId;
    let suggestions = req.body.suggestions;

    // var geometrystring = "st_geomfromtext('POINT(" + req.body.longitude + " " + req.body.latitude + ")',4326)";
    // var geometrystring = "st_geomfromtext('POINT(" + longitude + " " + latitude + ")',4326)";

    var querystring = "INSERT INTO bim_vis.asset_suggestions (object_id, suggestions) values ";
    querystring += "($1,$2) ";

    pool.connect(function (err, client, done) {
        if (err) {
            console.log("not able to get connection " + err);
            res.status(500).send(err);
        }
        client.query(querystring, [objectId, suggestions], function (err, result) {
            done();
            if (err) {
                console.log(err);
                res.status(500).send("You input a incorrect form with error: " + err);
            }
            else {
                res.status(200).send("Form Data " + req.body.objectId + " has been inserted");
            }
        });

    });

});


crud.post('/updateBim', function (req, res) {
    let objectId = req.body.objectId;
    let attribute = req.body.attribute;
    let modiValue = req.body.modiValue;
    let basecatego = req.body.basecatego;

    // var geometrystring = "st_geomfromtext('POINT(" + req.body.longitude + " " + req.body.latitude + ")',4326)";
    // var geometrystring = "st_geomfromtext('POINT(" + longitude + " " + latitude + ")',4326)";
    let tableName = categoryToTableMap[basecatego];

    if (!tableName) {
        return res.status(400).json({ success: false, message: 'Invalid category' });
    }

    // 构建SQL更新查询
    let querystring = `
        UPDATE ${tableName} 
        SET ${attribute} = $1
        WHERE objectid = $2
    `;
    console.log('tableName', tableName);
    console.log('basecatego',basecatego);
    console.log('attribute',attribute);

    pool.connect(function (err, client, done) {
        if (err) {
            console.log("not able to get connection " + err);
            res.status(500).send(err);
        }
        client.query(querystring, [modiValue, objectId], function (err, result) {
            done();
            if (err) {
                console.log(err);
                res.status(500).send("You input a incorrect data with error: " + err);
            }
            else {
                res.status(200).send("Object: " +  req.body.basecatego +"  "+ req.body.objectId + " has been updated. \nIt may take a few seconds to load the data.");
            }
        });

    });

});


module.exports = crud;
