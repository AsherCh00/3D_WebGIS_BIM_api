'use strict';
const supertest = require('supertest');
const request = supertest('http://localhost:4480'); 
const fs = require('fs');

test('retrieve asset information', async () => {
    const response = await request.get('/geojson24/asset_information');
      
    expect(response.status).toBe(200);

    // we use readFileSync here so that the system waits 
    // until the full file is loaded
    // an asychronous approach would mean that the code would try and run
    // the next line of text before the file is loaded
    //  the ./  means look in the same directory as this file
    // we use JSON.parse to convert the data into JSON for comparison
    let asset_information =  JSON.parse(fs.readFileSync("./js/asset_information.geojson"));
    expect(JSON.parse(response.text)).toEqual(asset_information);

  });
  

test('is valid JSON', async () => {
    const response = await request.get('/geojson24/getGeoJSON/cege0043/asset_information/id/location');
    expect(response.status).toBe(200);
    let isJSON = false;
    try {
        JSON.parse(response.text);
        isJSON = true;
    } catch (e) {
        isJSON = false;
    } 
    expect(isJSON).toEqual(true);
   
   });

/**
 * @function
 * @description test to see whether the returned JSON data is
 * <br> valid geoJSON - by looking for three key names
 * <br> geometry, type, properties
 * <br> note that we have to dig down into the JSON array to get these 
 * <br> values
 * @param {JSON} geoJSON  - the geoJSON data to be tested
 * @returns {boolean} - true if the test passed, false if it failed
 */
function testForValidGeoJSON(geoJSON){
	let isGeoJSON;
	try {
		Object.keys(geoJSON).forEach(function(key) {
			//console.log('Key : ' + key + ', Value : ' + geoJSON[key])
			let subGeoJSON = geoJSON[key];
  			Object.keys(subGeoJSON).forEach(function(key1){
  				//console.log('Key1 : ' + key + ', Value1 : ' + subGeoJSON[key1])
  				let subsubGeoJSON = subGeoJSON[key1];
			    Object.keys(subsubGeoJSON).forEach(function(key2){
  					//console.log('Key2 : ' + key2 + ', Value2 : ' + subsubGeoJSON[key2])
  					// at this level, the key should be one of
  					// geometry
  					// properties
  					// type - which should be feature
  					// if not this is not valid GeoJSON
  					if (key2 == 'properties' || key2=='geometry'|| (key2=='type'&& subsubGeoJSON[key2]=='Feature')){
  						isGeoJSON = true;
  					}
  					else {
  						// no need to continue
  						return false;
  					}
  				})
			})
		})
	} //end of the try
	catch (e){
		console.log(e.message);
		// an error has occured so return false
		return false;
	}
	// if we got to here, then the test has passed
	return isGeoJSON;
}

test('is geoJSON', async () => {
  	const response = await request.get('/geojson24/getGeoJSON/cege0043/asset_information/id/location');
	  expect(response.status).toBe(200);

	  let isGeoJSON = false;
	  let geoJSON = JSON.parse(response.text);
	  isGeoJSON = testForValidGeoJSON(geoJSON);
	  expect(isGeoJSON).toEqual(true);
 });

/**
 * @function extractColNames
 * @description parse geoJSON until the first instance of properties is found
 * <br> and use the keys in the properties data to get an array of
 * <br> column names in that geoJSON data
 * @param {JSON} geoJSON - the geoJSON data
 * @returns {Array} colNames - the array of column names
 */
function extractColNames(geoJSON){
	let colNames = []; // emptry array to store the column names
	try {
		let properties = geoJSON.features[0].properties;
		// now loop around this and get the key values
		Object.keys(properties).forEach(function(key) {
			colNames.push(key);
		});
	} //end of the try
	catch (e){
		console.log(e.message);
		// an error has occured so return false
		return false;
	}
	// if we got to here, then the test has passed
	return colNames;

}

/**
 * @function
 * @description take a list of all the tables and column names 
 * <br> in our database, and loop through and find the column names that 
 * <br> correspond to the given table and schema
 * <br> NB: we use schema as well as a table could appear with the same name
 * <br> in two different schemas
 * @param {String} tableName - the name of the table to check
 * @param {String} schemaName - the name of the schema to check
 * @returns {Array} informationSchema - the list of columns
 */
function extractColNamesInformationSchema(infoCols,schemaName,tableName){

	let infoColumns = []; //an empty array to store the results
	// now loop through to find entries where
	// table_schema = the schema name and
	// table_name = the table name
	// then get the column_name
	let infoColsJSON = JSON.parse(infoCols);
	Object.keys(infoColsJSON).forEach(function(key) {
		let infoSchema = infoColsJSON[key].table_schema;
		let infoTableName = infoColsJSON[key].table_name;
		if (infoTableName ==tableName && infoSchema ==schemaName) {
			// get hold of the column name
			infoColumns.push(infoColsJSON[key].column_name);
		} 
	});
	return infoColumns;
}

/** 
 * @function
 * @description - take an array of the matchingcolumn values from 
 * <br> the information schema query and compare them to the 
 * <br> property names in the GeoJSON file
 * @param {Array} colNames - the column names from the GeoJSON file
 * @param {Array} colNamesInformationSchema - the column names from the informatio
 * <br> schema query
 * @returns {Boolean} - true if all column names are found, false if not
 */
function compareColNames(colNames,colNamesInformationSchema){
	for (let i = 0;i<colNames.length;i++ ){
		let foundAMatch = false;
		for (let j= 0;j<colNamesInformationSchema.length;j++){
			if (colNames[i] == colNamesInformationSchema[j]){
				foundAMatch = true;
				// break out of the loop
				j = colNamesInformationSchema.length;
			}
		}
		// if we get to here and foundAMatch is false
		// then we didn't find a match for that column
		if (foundAMatch == false){
			return false;
		}
	}

	// if we get to here then match was found for all columns
	return true;
}

test('colNames correct', async () => {
const response = await request.get('/geojson24/getGeoJSON/cege0043/asset_information/id/location');
const infoCols = await request.get('/geojson24/postgistest');
let colNamesCorrect = false;
let geoJSON = JSON.parse(response.text);
let colNames = extractColNames(geoJSON);
let colNamesInformationSchema = extractColNamesInformationSchema(infoCols.text,'cege0043','asset_information');
colNamesCorrect = compareColNames(colNames,colNamesInformationSchema);
expect(colNamesCorrect).toEqual(true);
});