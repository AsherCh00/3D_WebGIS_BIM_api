"use strict";
/** Express router providing user related routes
 * @module dataAPI
 * @requires express
 * @requires path
 * 
 */

/**
 * express module
 * @description express is the server that forms part of the nodejs program
 * @const
 */
const express = require('express');


/**
 * path module
 * @description path is the module that logs the requests made to the router
 * @const
 */
const path = require("path");



/**
 * Express router to mount user related functions on.
 * @type {object}
 * @const
 * @namespace dataAPI
 */
const dataAPI = express();


// add an http server to serve files 	
let http = require('http');	
let httpServer = http.createServer(dataAPI);
let port = 4480;
let server = httpServer.listen(port);


// adding functionality to allow cross-domain queries
dataAPI.use(function(req, res, next) {
	res.setHeader("Access-Control-Allow-Origin", "*");
	res.setHeader("Access-Control-Allow-Headers", "X-Requested-With");
	res.setHeader('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE');
	next();
});


/**
 * Route serving basic test message
 * @name /
 * @function
 * @memberof module:dataAPI
 * @inner
 */
dataAPI.get('/',function (req,res) {
	res.send("hello world from the Data API on port: "+port);
});


// adding functionality to log the requests
dataAPI.use(function (req, res, next) {
	let filename = path.basename(req.url);
	let extension = path.extname(filename);
	console.log("The file " + filename + " was requested.");
	next();
});


/**
 * Route serving basic test message
 * @name /reversetext
 * @function
 * @memberof module:dataAPI
 * @inner
 * @param {string}  - the string to be reversed
 */
dataAPI.get('/reversetext',function (req,res) {
    //note, this is the buggy version for the students to fix
    let originalText = req.query.texttoreverse;
    console.log(originalText);

    console.log(req.originalUrl)

    // convert it to an array - we can use the built in Array.from for this
    let textArray = Array.from(originalText);
    console.log(textArray.length);

    let reversedText = "";
    // loop over the array backwards 
    // here i > 0 is the condition that, while true, will keep the loop
    // going - as soon as i > 0 returns false, the loop stops
    for (let i = textArray.length-1; i >= 0; i--){
        console.log(i);
		console.log('textArray = ' + textArray[i]);
        reversedText = reversedText + textArray[i];
		console.log('reversedText = ' + reversedText);
    }

    // finally send the result back to the browser
    res.send(reversedText);

});

const geoJSON = require('./routes/geoJSON');
const crud = require('./routes/crud');
dataAPI.use('/geojson24', geoJSON);
dataAPI.use('/crud24', crud);
dataAPI.use('/documentation', express.static(path.join(__dirname, 'documentation')));