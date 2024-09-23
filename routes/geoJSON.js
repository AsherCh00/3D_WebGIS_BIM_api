"use strict";
let express = require('express');
let geoJSON = require('express').Router();  // create a new route
let pg = require('pg'); // the code to connect to PostgreSQL
let fs = require('fs');  // code to read the database connection details file
let wkx = require('wkx');
let mongodb = require('mongodb');

// we need to get the user's name from the login used 
// to connect to the server
// that way we can work out the correct path for the connection file
//  /code/<<username>>/certs/
const os = require('os');
const username = os.userInfo().username;    // locate the database login details
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
console.log(config);

// TestGeoJSON end point
geoJSON.route('/testGeoJSON').get(function (req, res) {
    res.json({ message: req.originalUrl });
});

geoJSON.get('/postgistest', function (req, res) {

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
        let query = "select * from information_schema.columns";

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

// New end point: asset_information
geoJSON.get('/asset_information', function (req, res) {
    pool.connect(function (err, client, done) {
        if (err) {
            console.log("not able to get connection " + err);
            res.status(400).send(err);
        }
        let querystring = " SELECT 'FeatureCollection' As type, array_to_json(array_agg(f)) As features  FROM ";
        querystring = querystring + "(SELECT 'Feature' As type     , ST_AsGeoJSON(st_transform(lg.location,4326))::json As geometry, ";
        querystring = querystring + "row_to_json((SELECT l FROM (SELECT id, asset_name, installation_date, user_id, timestamp) As l      )) As properties";
        querystring = querystring + "   FROM cege0043.asset_information  As lg order by id limit 100  ) As f";
        console.log(querystring);
        client.query(querystring, function (err, result) {
            done();
            if (err) {
                console.log(err);
                res.status(400).send(err);
            }
            else {
                res.status(200).send(result.rows);
            }
        });
    });
});

// New end point: getGeoJSON
geoJSON.get('/getGeoJSON/:schemaname/:tablename/:idcolumn/:geomcolumn', function (req, res) {
    pool.connect(function (err, client, done) {
        if (err) {
            console.log("not able to get connection " + err);
            res.status(400).send(err);
        }

        let colnames = "";

        // first get a list of the columns that are in the table 
        // use string_agg to generate a comma separated list that can then be pasted into the next query
        let tablename = req.params.tablename;
        let schema = req.params.schemaname;
        let idcolumn = req.params.idcolumn;
        let geomcolumn = req.params.geomcolumn;
        let querystring = "select string_agg(colname,',') from ( select column_name as colname ";
        querystring = querystring + " FROM information_schema.columns as colname ";
        querystring = querystring + " where table_name   =$1";
        querystring = querystring + " and column_name <> $2 and table_schema = $3 and data_type <> 'USER-DEFINED') as cols ";
        console.log(querystring);

        // now run the query
        client.query(querystring, [tablename, geomcolumn, schema], function (err, result) {
            if (err) {
                console.log(err);
                res.status(400).send(err);
            }
            else {
                let thecolnames = result.rows[0].string_agg;
                colnames = thecolnames;
                console.log("the colnames " + thecolnames);

                let cols = colnames.split(",");
                let colString = "";
                for (let i = 0; i < cols.length; i++) {
                    // console.log(cols[i]);
                    colString = colString + JSON.stringify(cols[i]) + ",";
                }
                // console.log(colString);

                //remove the extra comma
                colString = colString.substring(0, colString.length - 1);

                // now use the inbuilt geoJSON functionality
                // and create the required geoJSON format using a query adapted from here:  
                // http://www.postgresonline.com/journal/archives/267-Creating-GeoJSON-Feature-Collections-with-JSON-and-PostGIS-functions.html, accessed 4th January 2018
                // note that query needs to be a single string with no line breaks so built it up bit by bit


                // to overcome the polyhedral surface issue, convert them to simple geometries
                // assume that all tables have an id field for now - to do add the name of the id field as a parameter
                querystring = "SELECT 'FeatureCollection' As type, array_to_json(array_agg(f)) As features  FROM ";
                querystring += "(select 'Feature' as type, x.properties,st_asgeojson(y.geometry)::json as geometry from ";
                querystring += " (select " + idcolumn + ", row_to_json((SELECT l FROM (SELECT " + colString + ") As l )) as properties   FROM " + schema + "." + JSON.stringify(tablename) + " ";


                querystring += " ) x";
                querystring += " inner join (SELECT " + idcolumn + ", c.geom as geometry";

                querystring += " FROM ( SELECT " + idcolumn + ", (ST_Dump(st_transform(" + JSON.stringify(geomcolumn) + ",4326))).geom AS geom ";

                querystring += " FROM " + schema + "." + JSON.stringify(tablename) + ") c) y  on y." + idcolumn + " = x." + idcolumn + ") f";
                // console.log(querystring);

                // run the second query
                client.query(querystring, function (err, result) {
                    //call `done()` to release the client back to the pool
                    done();
                    if (err) {


                        console.log(err);
                        res.status(400).send(err);
                    }
                    else {
                        // console.log(result.rows);
                        // the data from PostGIS is surrounded by [ ] which doesn't work in QGIS, so remove
                        // so we need to convert the JSON into a string temporarily
                        // remove the brackets and then convert it back when we send
                        // the result to the browser
                        let geoJSONData = JSON.stringify(result.rows);
                        geoJSONData = geoJSONData.substring(1);
                        geoJSONData = geoJSONData.substring(0, geoJSONData.length - 1);
                        // console.log(geoJSONData);
                        res.status(200).send(JSON.parse(geoJSONData));
                    }
                });
            } // end error check from client

        });
    });
});

geoJSON.get('/getQueueLengths', function (req, res) {
    pool.connect(function (err, client, done) {
        if (err) {
            console.log("not able to get connection " + err);
            res.status(400).send(err);
        }


        var querystring = "select * from cege0043.queue_length;";

        // run the second query
        client.query(querystring, function (err, result) {
            //call `done()` to release the client back to the pool
            done();
            if (err) {
                console.log(err);
                res.status(400).send(err);
            }
            else {
                // console.log(result.rows);
                let geoJSONData = JSON.stringify(result.rows);
                // console.log(geoJSONData);
                // geoJSONData = geoJSONData.substring(1);
                // geoJSONData = geoJSONData.substring(0, geoJSONData.length - 1);
                // // console.log(geoJSONData);
                res.status(200).send(result.rows);
            }
        });

    });

});

geoJSON.get('/petrolStationsByUser/:user_id', function (req, res) {


    pool.connect(function (err, client, done) {
        if (err) {
            console.log("not able to get connection " + err);
            res.status(400).send(err);
        }
        // first get a list of the columns that are in the table 
        // use string_agg to generate a comma separated list that can then be pasted into the next query
        let user_id = req.params.user_id;

        // to overcome the polyhedral surface issue, convert them to simple geometries
        // assume that all tables have an id field for now - to do add the name of the id field as a parameter

        let colnames = "petrol_station_id, petrol_station_name, last_inspected, petrol_station_latest_queue_length_and_price_date, queue_length_description, price_in_pounds";

        var querystring = " SELECT 'FeatureCollection' As type, array_to_json(array_agg(f)) As features  FROM ";
        querystring += "(SELECT 'Feature' As type     , ST_AsGeoJSON(lg.location)::json As geometry, ";
        querystring += "row_to_json((SELECT l FROM (SELECT " + colnames + " ) As l      )) As properties";
        querystring += "   FROM cege0043.petrol_station_latest_queue_length_and_price As lg ";
        querystring += " where user_id = $1 limit 100  ) As f ";

        // run the second query
        client.query(querystring, [user_id], function (err, result) {
            //call `done()` to release the client back to the pool
            done();
            if (err) {
                console.log(err);
                res.status(400).send(err);
            }
            else {
                // console.log(result.rows);
                // the data from PostGIS is surrounded by [ ] which doesn't work in QGIS, so remove
                // so we need to convert the JSON into a string temporarily
                // remove the brackets and then convert it back when we send
                // the result to the browser
                let geoJSONData = JSON.stringify(result.rows);
                geoJSONData = geoJSONData.substring(1);
                geoJSONData = geoJSONData.substring(0, geoJSONData.length - 1);
                // console.log(geoJSONData);
                res.status(200).send(JSON.parse(geoJSONData));
            }
        });
    });
});


geoJSON.get('/numPriceQueueReports/:user_id', function (req, res) {


    pool.connect(function (err, client, done) {
        if (err) {
            console.log("not able to get connection " + err);
            res.status(400).send(err);
        }


        // first get a list of the columns that are in the table 
        // use string_agg to generate a comma separated list that can then be pasted into the next query
        let user_id = req.params.user_id;

        // to overcome the polyhedral surface issue, convert them to simple geometries
        // assume that all tables have an id field for now - to do add the name of the id field as a parameter

        var querystring = "select array_to_json (array_agg(c)) ";
        querystring += "from ";
        querystring += "(SELECT COUNT(*) AS num_reports from cege0043.petrol_price_queue_information where user_id = $1) c;";



        // run the second query
        client.query(querystring, [user_id], function (err, result) {
            //call `done()` to release the client back to the pool
            done();
            if (err) {


                console.log(err);
                res.status(400).send(err);
            }
            else {
                // console.log(result.rows);
                // the data from PostGIS is surrounded by [ ] which doesn't work in QGIS, so remove
                // so we need to convert the JSON into a string temporarily
                // remove the brackets and then convert it back when we send
                // the result to the browser
                let geoJSONData = JSON.stringify(result.rows);
                geoJSONData = geoJSONData.substring(1);
                geoJSONData = geoJSONData.substring(0, geoJSONData.length - 1);
                // console.log(geoJSONData);
                res.status(200).send(JSON.parse(geoJSONData));
            }
        });

    });
});

geoJSON.get('/userPriceQueueRanking/:user_id', function (req, res) {
    pool.connect(function (err, client, done) {
        if (err) {
            console.log("not able to get connection " + err);
            res.status(400).send(err);
        }


        // first get a list of the columns that are in the table 
        // use string_agg to generate a comma separated list that can then be pasted into the next query
        let user_id = req.params.user_id;

        // to overcome the polyhedral surface issue, convert them to simple geometries
        // assume that all tables have an id field for now - to do add the name of the id field as a parameter

        var querystring = "select array_to_json (array_agg(hh)) ";
        querystring += "from ";
        querystring += "(select c.rank from (SELECT b.user_id, rank()over (order by num_reports desc) as rank ";
        querystring += "from (select COUNT(*) AS num_reports, user_id ";
        querystring += "from cege0043.petrol_price_queue_information ";
        querystring += "group by user_id) b) c ";
        querystring += "where c.user_id = $1) hh; ";
        querystring += " ";

        // run the second query
        client.query(querystring, [user_id], function (err, result) {
            //call `done()` to release the client back to the pool
            done();
            if (err) {


                console.log(err);
                res.status(400).send(err);
            }
            else {
                // console.log(result.rows);
                // the data from PostGIS is surrounded by [ ] which doesn't work in QGIS, so remove
                // so we need to convert the JSON into a string temporarily
                // remove the brackets and then convert it back when we send
                // the result to the browser
                let geoJSONData = JSON.stringify(result.rows);
                geoJSONData = geoJSONData.substring(1);
                geoJSONData = geoJSONData.substring(0, geoJSONData.length - 1);
                console.log('geoJSONData:\n' + geoJSONData);
                console.log(JSON.parse(geoJSONData));
                res.status(200).send(JSON.parse(geoJSONData));
            }
        });

    });
});

geoJSON.get('/petrolStationsWithFastQueues', function (req, res) {
    pool.connect(function (err, client, done) {
        if (err) {
            console.log("not able to get connection " + err);
            res.status(400).send(err);
        }


        // first get a list of the columns that are in the table 
        // use string_agg to generate a comma separated list that can then be pasted into the next query
        let user_id = req.params.user_id;

        // to overcome the polyhedral surface issue, convert them to simple geometries
        // assume that all tables have an id field for now - to do add the name of the id field as a parameter



        var querystring = "select array_to_json (array_agg(d)) from ";
        querystring += "(select c.* from cege0043.petrol_station c ";
        querystring += "inner join  ";
        querystring += "(select count(*) as fastest_queue, petrol_station_id from cege0043.petrol_price_queue_information  a where ";
        querystring += "queue_length_id in (select queue_length_id from cege0043.queue_length ";
        querystring += "where queue_length_description like '%very short%' or queue_length_description like '%No queue%') ";
        querystring += "group by petrol_station_id ";
        querystring += "order by fastest_queue desc) b ";
        querystring += "on b.petrol_station_id = c.petrol_station_id) d; ";
        querystring += " ";


        // run the second query
        client.query(querystring, function (err, result) {
            //call `done()` to release the client back to the pool
            done();
            if (err) {


                console.log(err);
                res.status(400).send(err);
            }
            else {
                // console.log(result.rows);
                // the data from PostGIS is surrounded by [ ] which doesn't work in QGIS, so remove
                // so we need to convert the JSON into a string temporarily
                // remove the brackets and then convert it back when we send
                // the result to the browser
                let geoJSONData = JSON.stringify(result.rows);
                geoJSONData = geoJSONData.substring(1);
                geoJSONData = geoJSONData.substring(0, geoJSONData.length - 1);
                // console.log(geoJSONData);
                res.status(200).send(JSON.parse(geoJSONData));
            }
        });

    });
});

geoJSON.get('/petrolStationsByQueueLength', function (req, res) {
    pool.connect(function (err, client, done) {
        if (err) {
            console.log("not able to get connection " + err);
            res.status(400).send(err);
        }

        var querystring = "select count(*) as num_petrol_stations, queue_length_description from ";
        querystring += "cege0043.petrol_station_latest_queue_length_and_price ";
        querystring += "where queue_length_description is not null ";
        querystring += "group by queue_length_description; ";
        querystring += " ";


        // run the second query
        client.query(querystring, function (err, result) {
            //call `done()` to release the client back to the pool
            done();
            if (err) {


                console.log(err);
                res.status(400).send(err);
            }
            else {
                // console.log(result.rows);
                // the data from PostGIS is surrounded by [ ] which doesn't work in QGIS, so remove
                // so we need to convert the JSON into a string temporarily
                // remove the brackets and then convert it back when we send
                // the result to the browser

                let geoJSONData = JSON.stringify(result.rows);
                geoJSONData = geoJSONData.substring(1);
                geoJSONData = geoJSONData.substring(0, geoJSONData.length - 1);
                console.log('geoJSONData:\n' + geoJSONData);
                // console.log(JSON.parse(geoJSONData));
                // Need [] for this GeoJSON
                geoJSONData = '[' + geoJSONData + ']'
                res.status(200).send(JSON.parse(geoJSONData));
            }
        });
    });
});

geoJSON.get('/fiveClosestPetrolStations/:latitude/:longitude', function (req, res) {
    pool.connect(function (err, client, done) {
        if (err) {
            console.log("not able to get connection " + err);
            res.status(400).send(err);
        }
        let latitude = req.params.latitude;
        let longitude = req.params.longitude;

        var querystring = "SELECT 'FeatureCollection' As type, array_to_json(array_agg(f)) As features  FROM ";
        querystring += "(SELECT 'Feature' As type     , ST_AsGeoJSON(lg.location)::json As geometry, ";
        querystring += "row_to_json((SELECT l FROM (SELECT petrol_station_id, petrol_station_name, last_inspected) As l ";
        querystring += " )) As properties ";
        querystring += "FROM   (select c.* from cege0043.petrol_station c ";
        querystring += "inner join (select petrol_station_id, st_distance(a.location, st_geomfromtext('POINT("
        querystring += longitude + " " + latitude + ")',4326)) as distance ";
        querystring += "from cege0043.petrol_station a ";
        querystring += "order by distance asc ";
        querystring += "limit 5) b ";
        querystring += "on c.petrol_station_id = b.petrol_station_id ) as lg) As f; ";
        querystring += " ";


        // run the second query
        client.query(querystring, function (err, result) {
            //call `done()` to release the client back to the pool
            done();
            if (err) {


                console.log(err);
                res.status(400).send(err);
            }
            else {
                let geoJSONData = JSON.stringify(result.rows);
                geoJSONData = geoJSONData.substring(1);
                geoJSONData = geoJSONData.substring(0, geoJSONData.length - 1);
                res.status(200).send(JSON.parse(geoJSONData));
            }
        });

    });
});


geoJSON.get('/petrolStationsQueueLengthUnknown', function (req, res) {

    let user_id = req.query.user_id;

    pool.connect(function (err, client, done) {
        if (err) {
            console.log("not able to get connection " + err);
            res.status(400).send(err);
        }

        var querystring = "select * from cege0043.petrol_station_latest_queue_length_and_price ";
        querystring += "where queue_length_description  = 'Unknown' ";
        querystring += "and user_id = $1; ";
        querystring += " ";

        // let serviceUrl = document.location.origin + "/api/crud24/userID";


        // run the second query
        client.query(querystring, [user_id], function (err, result) {
            //call `done()` to release the client back to the pool
            done();
            if (err) {


                console.log(err);
                res.status(400).send(err);
            }
            else {
                // console.log(result.rows);
                // the data from PostGIS is surrounded by [ ] which doesn't work in QGIS, so remove
                // so we need to convert the JSON into a string temporarily
                // remove the brackets and then convert it back when we send
                // the result to the browser
                let geoJSONData = JSON.stringify(result.rows);
                geoJSONData = geoJSONData.substring(1);
                geoJSONData = geoJSONData.substring(0, geoJSONData.length - 1);
                // console.log(JSON.parse(geoJSONData));
                // Need [] for this GeoJSON

                geoJSONData = '[' + geoJSONData + ']'
                geoJSONData = JSON.parse(geoJSONData)
                console.log(geoJSONData[0]);
                console.log(geoJSONData[0].location);
                // console.log("KKK1",convertWKBtoLatLng(geoJSONData[0].location))
                for (let i = 0; i < geoJSONData.length; i++) {
                    geoJSONData[i].coordinates = convertWKBtoLatLng(geoJSONData[i].location);
                    console.log(geoJSONData[i]);

                }
                res.status(200).send(geoJSONData);
            }
        });

    });
});

function convertWKBtoLatLng(wkb) {
    const buffer = Buffer.from(wkb, 'hex');
    const geometry = wkx.Geometry.parse(buffer);
    console.log(geometry);
    return [geometry.y, geometry.x];  // lat, lng
}

geoJSON.get('/getStationsWithRecentPriceQueueReportMissing/:user_id', function (req, res) {
    pool.connect(function (err, client, done) {
        if (err) {
            console.log("not able to get connection " + err);
            res.status(400).send(err);
        }
        let user_id = req.params.user_id;

        var querystring = "SELECT 'FeatureCollection' As type, array_to_json(array_agg(f)) As features  FROM ";
        querystring += "(SELECT 'Feature' As type     , ST_AsGeoJSON(lg.location)::json As geometry, ";
        querystring += "row_to_json((SELECT l FROM (SELECT petrol_station_id, petrol_station_name, last_inspected, ";
        querystring += "petrol_station_latest_queue_length_and_price_date, price_in_pounds, queue_length_description ";
        querystring += ") As l )) ";
        querystring += "As properties FROM  ";
        querystring += "(select * from cege0043.petrol_station_latest_queue_length_and_price ";
        querystring += "where "
        querystring += "user_id = $1 and ";
        querystring += "petrol_station_id not in ( ";
        querystring += "select petrol_station_id from cege0043.petrol_price_queue_information ";
        querystring += "where "
        querystring += "user_id = $1 and ";
        querystring += "timestamp > NOW()::DATE-EXTRACT(DOW FROM NOW())::INTEGER-3)  ) as lg) As f ";
        querystring += " ";

        // user_id = 583 to test


        // run the second query
        // client.query(querystring,  function (err, result) {
        client.query(querystring, [user_id], function (err, result) {
            //call `done()` to release the client back to the pool
            done();
            if (err) {


                console.log(err);
                res.status(400).send(err);
            }
            else {
                let geoJSONData = JSON.stringify(result.rows);
                geoJSONData = geoJSONData.substring(1);
                geoJSONData = geoJSONData.substring(0, geoJSONData.length - 1);
                res.status(200).send(JSON.parse(geoJSONData));
            }
        });

    });
});


geoJSON.get('/petrolStationLatestPrices/:user_id', function (req, res) {
    pool.connect(function (err, client, done) {
        if (err) {
            console.log("not able to get connection " + err);
            res.status(400).send(err);
        }
        let user_id = req.params.user_id;

        var querystring = "select * from cege0043.petrol_station_latest_queue_length_and_price ";
        querystring += "where user_id = $1; ";

        // run the second query
        // client.query(querystring,  function (err, result) {
        client.query(querystring, [user_id], function (err, result) {
            //call `done()` to release the client back to the pool
            done();
            if (err) {


                console.log(err);
                res.status(400).send(err);
            }
            else {
                // console.log(result.rows)
                // let geoJSONData = JSON.stringify(result.rows);
                // console.log(geoJSONData)
                // geoJSONData = geoJSONData.substring(1);
                // geoJSONData = geoJSONData.substring(0, geoJSONData.length - 1);
                res.status(200).send(result.rows);
            }
        });

    });
});


// Office Bim GeoJSON

geoJSON.get('/office_ceilings', function (req, res) {
    pool.connect(function (err, client, done) {
        if (err) {
            console.log("not able to get connection " + err);
            res.status(400).send(err);
        }

        // var queryString = "SELECT id, basecatego, discipline, category, elementtyp, ";
        // queryString += "family, objectid, docname, heightleve, ST_AsBinary(geometry) as geom FROM bim_vis.office_ceilings";

        // var queryString = "SELECT 'FeatureCollection' As type, array_to_json(array_agg(f)) As features "
        // queryString += "FROM (SELECT 'Feature' As type, ST_AsGeoJSON(st_transform(geometry, 4326))::json As geometry, "
        // queryString += "row_to_json( (SELECT l FROM (SELECT id, basecatego, discipline, category, "
        // queryString += "elementtyp, family, objectid, docname, heightleve) As l )) As properties "
        // queryString += "FROM bim_vis.office_ceilings ORDER BY id) As f; "

        let colnames = "BaseCatego, Discipline, Category, ElementTyp, Family, HeightLeve, BldgLevel, CreatedPha, ";
        colnames += "Demolished, Function, Mark, ObjectId, ";
        colnames += "Typ_Mark, Type, DocName, DocType, DocUpdate, Bldg_Name, DocId, DocVer, ";
        colnames += "BldgLevel_, BldgLevel1, BldgLeve_1, BldgLeve_2, Client_Nam, ExtId, FamilyType, ";
        colnames += "Keynote, Project_Ad, Project_Is, Project_Na, Project_Nu, Project_St ";


        var queryString = "SELECT 'FeatureCollection' As type, array_to_json(array_agg(f)) As features ";
        queryString += "FROM (SELECT 'Feature' As type, ST_AsGeoJSON(st_transform(geometry, 4326))::json As geometry, ";
        queryString += "row_to_json( (SELECT l FROM (SELECT " + colnames + " ) As l )) As properties ";
        queryString += "FROM bim_vis.office_ceilings ORDER BY id) As f; ";



        console.log(queryString);
        client.query(queryString, function (err, result) {
            //call `done()` to release the client back to the pool
            done();
            if (err) {
                console.log(err);
                res.status(400).send(err);
            } else {
                let geoJSONData = JSON.stringify(result.rows);
                geoJSONData = geoJSONData.substring(1);
                geoJSONData = geoJSONData.substring(0, geoJSONData.length - 1);
                res.status(200).send(JSON.parse(geoJSONData));
            }
        });
    });
});

geoJSON.get('/office_doors', function (req, res) {
    pool.connect(function (err, client, done) {
        if (err) {
            console.log("not able to get connection " + err);
            res.status(400).send(err);
        }

        let colnames = "BaseCatego, Discipline, Category, ElementTyp, Family, HeightLeve, BldgLevel, CreatedPha, ";
        colnames += "Demolished, Function, Mark, ObjectId, ";
        colnames += "Typ_Mark, Type, DocName, DocType, DocUpdate, Bldg_Name, DocId, ";
        colnames += "BaseLevel, BaseLevel_, BaseLevel1, BaseLeve_1, BaseLeve_2, ";
        colnames += "BldgLevel_, BldgLevel1, BldgLeve_1, BldgLeve_2, Client_Nam, Descriptio, ExtId, Fam_Descri, ";
        colnames += "Fam_Keynot, Fam_Manufa, Fam_Operat, Fam_Type_C, FamilyType, HostFeatur, ";
        colnames += "HostId, InstanceEl, Keynote, Manufactur, Project_Ad, Project_Is, Project_Na, Project_Nu, Project_St, ";
        colnames += "Thickness, Top_Cut_St, Top_Extens, TopLevel, TopLevel_D, TopLevel_E, TopLevel_I, ";
        colnames += "TopLevel_R, Typ_Descri, Typ_Keynot, Typ_Manufa, Typ_Operat, Typ_Typ_Ma, Typ_Type_C, Type_Comme ";

        var queryString = "SELECT 'FeatureCollection' As type, array_to_json(array_agg(f)) As features "
        queryString += "FROM (SELECT 'Feature' As type, ST_AsGeoJSON(st_transform(geometry, 4326))::json As geometry, "
        queryString += "row_to_json( (SELECT l FROM (SELECT " + colnames + " ) As l )) As properties "
        queryString += "FROM bim_vis.office_doors ORDER BY id) As f; "

        console.log(queryString);
        client.query(queryString, function (err, result) {
            //call `done()` to release the client back to the pool
            done();
            if (err) {
                console.log(err);
                res.status(400).send(err);
            } else {
                let geoJSONData = JSON.stringify(result.rows);
                geoJSONData = geoJSONData.substring(1);
                geoJSONData = geoJSONData.substring(0, geoJSONData.length - 1);
                res.status(200).send(JSON.parse(geoJSONData));
            }
        });
    });
});


geoJSON.get('/office_floors', function (req, res) {
    pool.connect(function (err, client, done) {
        if (err) {
            console.log("not able to get connection " + err);
            res.status(400).send(err);
        }

        let colnames = "BaseCatego, Discipline, Category, ElementTyp, Family, HeightLeve, BldgLevel, CreatedPha, ";
        colnames += "Demolished, Function, Mark, ObjectId, ";
        colnames += "Typ_Mark, Type, DocName, DocType, DocUpdate, Bldg_Name, DocId, DocVer, ";
        colnames += "BldgLevel_, BldgLevel1, BldgLeve_1, BldgLeve_2, Client_Nam, ExtId, FamilyType, IsStructur, ";
        colnames += "Keynote, Project_Ad, Project_Is, Project_Na, Project_Nu, Project_St, Structural ";

        let queryString = "SELECT 'FeatureCollection' As type, array_to_json(array_agg(f)) As features ";
        queryString += "FROM (SELECT 'Feature' As type, ST_AsGeoJSON(st_transform(geometry, 4326))::json As geometry, ";
        queryString += "row_to_json( (SELECT l FROM (SELECT " + colnames + " ) As l )) As properties ";
        queryString += "FROM bim_vis.office_floors ORDER BY id) As f; ";

        console.log(queryString);
        client.query(queryString, function (err, result) {
            //call `done()` to release the client back to the pool
            done();
            if (err) {
                console.log(err);
                res.status(400).send(err);
            } else {
                let geoJSONData = JSON.stringify(result.rows);
                geoJSONData = geoJSONData.substring(1);
                geoJSONData = geoJSONData.substring(0, geoJSONData.length - 1);
                res.status(200).send(JSON.parse(geoJSONData));
            }
        });
    });
});

geoJSON.get('/office_walls', function (req, res) {
    pool.connect(function (err, client, done) {
        if (err) {
            console.log("not able to get connection " + err);
            res.status(400).send(err);
        }

        let colnames = "BaseCatego, Discipline, Category, ElementTyp, Family, BldgLevel, CreatedPha, ";
        colnames += "Demolished, Function, Mark, ObjectId, ";
        colnames += "Typ_Mark, Type, DocName, DocType, DocUpdate, Bldg_Name, DocId, BaseOffset, ";
        colnames += "BldgLevel_, BldgLevel1, BldgLeve_1, BldgLeve_2, Client_Nam, ExtId, FamilyType, ";
        colnames += "IsStructur, Keynote, Project_Ad, Project_Is, Project_Na, Project_Nu, Project_St, ";
        colnames += "Structural, Structur_1, TopOffset, UpToLevel, UpToLevel_, UpToLevel1, UpToLeve_1, UpToLeve_2, Wall_Kind ";

        let queryString = "SELECT 'FeatureCollection' As type, array_to_json(array_agg(f)) As features ";
        queryString += "FROM (SELECT 'Feature' As type, ST_AsGeoJSON(st_transform(geometry, 4326))::json As geometry, ";
        queryString += "row_to_json( (SELECT l FROM (SELECT " + colnames + " ) As l )) As properties ";
        queryString += "FROM bim_vis.office_walls ORDER BY id) As f; ";


        console.log(queryString);
        client.query(queryString, function (err, result) {
            //call `done()` to release the client back to the pool
            done();
            if (err) {
                console.log(err);
                res.status(400).send(err);
            } else {
                let geoJSONData = JSON.stringify(result.rows);
                geoJSONData = geoJSONData.substring(1);
                geoJSONData = geoJSONData.substring(0, geoJSONData.length - 1);
                res.status(200).send(JSON.parse(geoJSONData));
            }
        });
    });
});


geoJSON.get('/office_windows', function (req, res) {
    pool.connect(function (err, client, done) {
        if (err) {
            console.log("not able to get connection " + err);
            res.status(400).send(err);
        }

        let colnames = "BaseCatego, Discipline, Category, ElementTyp, Family, HeightLeve, BldgLevel, CreatedPha, ";
        colnames += "Demolished, Function, Mark, ObjectId, ";
        colnames += "Typ_Mark, Type, DocName, DocType, DocUpdate, Bldg_Name, DocId, ";
        colnames += "BaseLevel, BaseLevel_, BaseLevel1, BaseLeve_1, BaseLeve_2, ";
        colnames += "BldgLevel_, BldgLevel1, BldgLeve_1, BldgLeve_2, Client_Nam, ExtId, Fam_Keynot, Fam_Manufa, ";
        colnames += "Fam_Operat, FamilyType, InstanceEl, Keynote, ";
        colnames += "Manufactur, Project_Ad, Project_Is, Project_Na, Project_Nu, Project_St, Top_Cut_St, ";
        colnames += "Top_Extens, TopLevel, TopLevel_D, TopLevel_E, TopLevel_I, TopLevel_R, Typ_Keynot, ";
        colnames += "Typ_Manufa, Typ_Operat, Typ_Typ_Ma ";

        let queryString = "SELECT 'FeatureCollection' As type, array_to_json(array_agg(f)) As features ";
        queryString += "FROM (SELECT 'Feature' As type, ST_AsGeoJSON(st_transform(geometry, 4326))::json As geometry, ";
        queryString += "row_to_json( (SELECT l FROM (SELECT " + colnames + " ) As l )) As properties ";
        queryString += "FROM bim_vis.office_windows ORDER BY id) As f; ";


        console.log(queryString);
        client.query(queryString, function (err, result) {
            //call `done()` to release the client back to the pool
            done();
            if (err) {
                console.log(err);
                res.status(400).send(err);
            } else {
                let geoJSONData = JSON.stringify(result.rows);
                geoJSONData = geoJSONData.substring(1);
                geoJSONData = geoJSONData.substring(0, geoJSONData.length - 1);
                res.status(200).send(JSON.parse(geoJSONData));
            }
        });
    });
});

geoJSON.get('/office_room', function (req, res) {
    pool.connect(function (err, client, done) {
        if (err) {
            console.log("not able to get connection " + err);
            res.status(400).send(err);
        }

        // var queryString = "SELECT id, basecatego, discipline, category, elementtyp, "
        // queryString += "family, objectid, docname, ST_AsBinary(geometry) as geom FROM bim_vis.office_rooms "
        // queryString += "WHERE objectid = '1359163'"

        let colnames = "BaseCatego, Discipline, Category, ElementTyp, Family, BldgLevel, CreatedPha, ";
        colnames += "Demolished, Function, Mark, ObjectId, ";
        colnames += "Typ_Mark, Type, DocName, DocType, DocUpdate, Bldg_Name, DocId, ";
        colnames += "BldgLevel_, BldgLevel1, BldgLeve_1, BldgLeve_2, Client_Nam, ExtId, Occupancy, Project_Ad, ";
        colnames += "Project_Is, Project_Na, Project_Nu, Project_St, RoomName, RoomNumber, SourceArea, SourceHeig, SourcePeri ";

        let queryString = "SELECT 'FeatureCollection' AS type, array_to_json(array_agg(f)) AS features ";
        queryString += "FROM (SELECT 'Feature' AS type, ST_AsGeoJSON(ST_Transform(geometry, 4326))::json AS geometry, ";
        queryString += "row_to_json( (SELECT l FROM (SELECT " + colnames + " ) As l )) As properties ";
        queryString += "FROM bim_vis.office_rooms WHERE objectid = '1359163') As f; ";


        console.log(queryString);
        client.query(queryString, function (err, result) {
            //call `done()` to release the client back to the pool
            done();
            if (err) {
                console.log(err);
                res.status(400).send(err);
            } else {
                let geoJSONData = JSON.stringify(result.rows);
                geoJSONData = geoJSONData.substring(1);
                geoJSONData = geoJSONData.substring(0, geoJSONData.length - 1);
                res.status(200).send(JSON.parse(geoJSONData));
            }
        });
    });
});


geoJSON.get('/room_furniture', function (req, res) {
    pool.connect(function (err, client, done) {
        if (err) {
            console.log("not able to get connection " + err);
            res.status(400).send(err);
        }

        let colnames = "BaseCatego, Discipline, Category, ElementTyp, Family, HeightLeve, BldgLevel, CreatedPha, ";
        colnames += "Demolished, Function, Mark, ObjectId, ";
        colnames += "Typ_Mark, Type, DocName, DocType, DocUpdate, Bldg_Name, DocId, ";
        colnames += "BaseLevel, BaseLevel_, BaseLevel1, BaseLeve_1, BaseLeve_2, ";
        colnames += "BldgLevel_, BldgLevel1, BldgLeve_1, BldgLeve_2, Client_Nam, ExtId, ";
        colnames += "FamilyType, HostFeatur, HostId, ";
        colnames += "InstanceEl, Keynote, Project_Ad, Project_Is, Project_Na, Project_Nu, Project_St, ";
        colnames += "Top_Cut_St, Top_Extens, TopLevel, TopLevel_D, TopLevel_E, TopLevel_I ";

        let queryString = "WITH rooms_2d AS (SELECT objectid, ST_Force2D(geometry) AS geometry_2d FROM bim_vis.office_rooms WHERE objectid = '1359163'), ";
        queryString += "furnitures_2d AS (SELECT objectid, baseLevel_, ST_Force2D(geometry) AS geometry_2d FROM bim_vis.office_furnitures), ";
        queryString += "computers_2d AS (SELECT objectid, baseLevel_, ST_Force2D(geometry) AS geometry_2d FROM bim_vis.office_computers), ";
        queryString += "rooms_bbox AS (SELECT objectid, ST_Envelope(geometry_2d) AS bbox FROM rooms_2d), ";
        queryString += "furnitures_bbox AS (SELECT objectid, baseLevel_, ST_Envelope(geometry_2d) AS bbox FROM furnitures_2d), ";
        queryString += "computers_bbox AS (SELECT objectid, baseLevel_, ST_Envelope(geometry_2d) AS bbox FROM computers_2d), ";

        queryString += "contained_furnitures AS (SELECT f.objectid FROM furnitures_bbox f JOIN rooms_bbox r ";
        queryString += "ON ST_Contains(r.bbox, f.bbox) WHERE r.objectid = '1359163' AND f.baseLevel_ = 'Floor 4'), ";
        queryString += "contained_computers AS ( SELECT c.objectid FROM computers_bbox c JOIN rooms_bbox r ";
        queryString += "ON ST_Contains(r.bbox, c.bbox) WHERE r.objectid = '1359163' AND c.baseLevel_ = 'Floor 4') ";

        queryString += "SELECT 'FeatureCollection' AS type, array_to_json(array_agg(f)) AS features FROM ( ";
        queryString += "SELECT 'Feature' AS type, ST_AsGeoJSON(ST_Transform(geometry, 4326))::json AS geometry, row_to_json((SELECT l FROM ( ";
        queryString += "SELECT " + colnames + " ) AS l)) AS properties ";
        queryString += "FROM bim_vis.office_furnitures WHERE objectid IN (SELECT objectid FROM contained_furnitures) ";
        queryString += "UNION ALL "
        queryString += "SELECT 'Feature' AS type, ST_AsGeoJSON(ST_Transform(geometry, 4326))::json AS geometry, row_to_json((SELECT l FROM ( ";
        queryString += "SELECT " + colnames + " ) AS l)) AS properties ";
        queryString += "FROM bim_vis.office_computers WHERE objectid IN (SELECT objectid FROM contained_computers)) AS f; ";


        console.log(queryString);
        client.query(queryString, function (err, result) {
            //call `done()` to release the client back to the pool
            done();
            if (err) {
                console.log(err);
                res.status(400).send(err);
            } else {
                let geoJSONData = JSON.stringify(result.rows);
                geoJSONData = geoJSONData.substring(1);
                geoJSONData = geoJSONData.substring(0, geoJSONData.length - 1);
                res.status(200).send(JSON.parse(geoJSONData));
            }
        });
    });
});

geoJSON.get('/room_furniture_forView', function (req, res) {
    pool.connect(function (err, client, done) {
        if (err) {
            console.log("not able to get connection " + err);
            res.status(400).send(err);
        }

        let colnames = "BaseCatego, Discipline, Category, ElementTyp, Family, HeightLeve, ObjectId, DocName ";

        let queryString = "WITH rooms_2d AS (SELECT objectid, ST_Force2D(geometry) AS geometry_2d FROM bim_vis.office_rooms WHERE objectid = '361511'), ";
        queryString += "furnitures_2d AS (SELECT objectid, baseLevel_, ST_Force2D(geometry) AS geometry_2d FROM bim_vis.office_furnitures), ";
        queryString += "computers_2d AS (SELECT objectid, baseLevel_, ST_Force2D(geometry) AS geometry_2d FROM bim_vis.office_computers), ";
        queryString += "rooms_bbox AS (SELECT objectid, ST_Envelope(geometry_2d) AS bbox FROM rooms_2d), ";
        queryString += "furnitures_bbox AS (SELECT objectid, baseLevel_, ST_Envelope(geometry_2d) AS bbox FROM furnitures_2d), ";
        queryString += "computers_bbox AS (SELECT objectid, baseLevel_, ST_Envelope(geometry_2d) AS bbox FROM computers_2d), ";

        queryString += "contained_furnitures AS (SELECT f.objectid FROM furnitures_bbox f JOIN rooms_bbox r ";
        queryString += "ON ST_Contains(r.bbox, f.bbox) WHERE r.objectid = '361511' AND f.baseLevel_ = 'Floor 4'), ";
        queryString += "contained_computers AS ( SELECT c.objectid FROM computers_bbox c JOIN rooms_bbox r ";
        queryString += "ON ST_Contains(r.bbox, c.bbox) WHERE r.objectid = '361511' AND c.baseLevel_ = 'Floor 4') ";

        queryString += "SELECT 'FeatureCollection' AS type, array_to_json(array_agg(f)) AS features FROM ( ";
        queryString += "SELECT 'Feature' AS type, ST_AsGeoJSON(ST_Transform(geometry, 4326))::json AS geometry, row_to_json((SELECT l FROM ( ";
        queryString += "SELECT " + colnames + " ) AS l)) AS properties ";
        queryString += "FROM bim_vis.office_furnitures WHERE objectid IN (SELECT objectid FROM contained_furnitures) ";
        queryString += "UNION ALL "
        queryString += "SELECT 'Feature' AS type, ST_AsGeoJSON(ST_Transform(geometry, 4326))::json AS geometry, row_to_json((SELECT l FROM ( ";
        queryString += "SELECT " + colnames + " ) AS l)) AS properties ";
        queryString += "FROM bim_vis.office_computers WHERE objectid IN (SELECT objectid FROM contained_computers)) AS f; ";


        console.log(queryString);
        client.query(queryString, function (err, result) {
            //call `done()` to release the client back to the pool
            done();
            if (err) {
                console.log(err);
                res.status(400).send(err);
            } else {
                let geoJSONData = JSON.stringify(result.rows);
                geoJSONData = geoJSONData.substring(1);
                geoJSONData = geoJSONData.substring(0, geoJSONData.length - 1);
                res.status(200).send(JSON.parse(geoJSONData));
            }
        });
    });
});

geoJSON.get('/benchmark_postgresql', function (req, res) {
    console.time("Total Execution Time"); // 开始计时
    pool.connect(function (err, client, done) {
        if (err) {
            console.log("not able to get connection " + err);
            res.status(400).send(err);
        }

        let colnames = "BaseCatego, Discipline, Category, ElementTyp, Family, HeightLeve, ObjectId, DocName, DocType, DocUpdate ";

        // All properties
        // let colnames = "BaseCatego, Discipline, AssemblyCo, AssemblyDe, BldgLevel, Category, CreatedPha, ";
        // colnames += "Demolished, ElementTyp, Family, Function, Mark, ObjectId, OmniClass, OmniClassD, ";
        // colnames += "Typ_Mark, Type, DocName, DocPath, DocType, DocUpdate, Bldg_Name, DocId, ";
        // colnames += "Base_Cut_S, Base_Exten, BaseLevel, BaseLevel_, BaseLevel1, BaseLeve_1, BaseLeve_2, ";
        // colnames += "BldgLevel_, BldgLevel1, BldgLeve_1, BldgLeve_2, Client_Nam, Descriptio, ExtId, ";
        // colnames += "Fam_Descri, Fam_Keynot, FamilyType, HeightLeve, InstanceEl, Keynote, ";
        // colnames += "Project_Ad, Project_Is, Project_Na, Project_Nu, Project_St, Top_Cut_St, Top_Extens, ";
        // colnames += "TopLevel, TopLevel_D, TopLevel_E, TopLevel_I, TopLevel_R, Typ_Descri, Typ_Keynot, ";
        // colnames += "Fam_Assemb, Typ_Assemb, HostFeatur, HostId ";




        // let queryString = 'SELECT id, basecatego, discipline, category, element_typ, ';
        // queryString += 'family, objectid, docname, heightleve, ST_AsBinary(geom) as geom FROM bim_vis.office_features ';
        // queryString += "where family = 'Furniture_Desk' or family = 'Furniture_Table_Dining_w-Chairs_Round' ";
        // queryString += "or family = 'Furniture_Table_Conference_w-Chairs' ORDER BY family LIMIT 372;";

        // queryString += "SELECT 'FeatureCollection' AS type, array_to_json(array_agg(f)) AS features FROM ( ";
        // queryString += "SELECT 'Feature' AS type, ST_AsGeoJSON(ST_Transform(geometry, 4326))::json AS geometry, row_to_json((SELECT l FROM ( ";
        // queryString += "SELECT " + colnames + " ) AS l)) AS properties ";
        // queryString += "FROM bim_vis.office_furnitures WHERE objectid IN (SELECT objectid FROM contained_furnitures) ";

        let queryString = "SELECT 'FeatureCollection' AS type, array_to_json(array_agg(f)) AS features ";
        queryString += "FROM (SELECT 'Feature' AS type, ST_AsGeoJSON(ST_Transform(geometry, 4326))::json AS geometry, ";
        queryString += "row_to_json((SELECT l FROM (SELECT " + colnames + " ) As l )) As properties ";
        queryString += "FROM bim_vis.office_furnitures ";
        queryString += "WHERE family = 'Furniture_Desk' or family = 'Furniture_Table_Dining_w-Chairs_Round' or family = 'Furniture_Table_Conference_w-Chairs' "
        queryString += "ORDER BY family LIMIT 348 ) As f; ";
        // queryString += "WHERE family = 'Furniture_Desk' LIMIT 348 ) As f; ";

        console.log(queryString);
        client.query(queryString, function (err, result) {
            //call `done()` to release the client back to the pool
            done();
            if (err) {
                console.log(err);
                res.status(400).send(err);
            } else {
                let geoJSONData = JSON.stringify(result.rows);
                geoJSONData = geoJSONData.substring(1);
                geoJSONData = geoJSONData.substring(0, geoJSONData.length - 1);
                res.status(200).send(JSON.parse(geoJSONData));
            }
            console.timeEnd("Total Execution Time"); 
        });
    });
});


const mongoURI = 'mongodb://localhost:27017/bim_vis';

// MongoDB 
const databaseName = 'bim_vis';
const collectionName = 'benchmark_250';

geoJSON.get('/benchmark_mongodb', async function (req, res) {
    console.time("Total Execution Time"); 
    let client = new mongodb.MongoClient(mongoURI);

    try {
        await client.connect();
        console.log('MongoDB connected');

        const mongoDatabase = client.db(databaseName);
        const collection = mongoDatabase.collection(collectionName);

        const geojsonDocuments = await collection.find({'properties.Family': 'Furniture_Desk'}).limit(50).toArray();

        const geojson = {
            type: "FeatureCollection",
            features: geojsonDocuments.map(doc => {
                return {
                    type: "Feature",
                    properties: doc.properties, 
                    geometry: doc.geometry 
                    
                };
            })
        };

        res.json(geojson);

    } catch (err) {
        console.error('Error:', err);
        res.status(500).send(err);
    } finally {
        if (client) {
            await client.close();
        }
        console.timeEnd("Total Execution Time"); 
    }
});


module.exports = geoJSON;

