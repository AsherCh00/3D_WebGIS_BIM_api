'use strict';
const supertest = require('supertest');
const request = supertest('http://localhost:4480');
const os = require('os');
const fs = require('fs');
const username = os.userInfo().username;    // locate the database login details

test('returns a JSON message', async () => {
  const dirTest = '/crud24/testCRUD?name=echo GET REQUEST'
  const response = await request.get('/crud24/testCRUD?name=echo');
  expect(response.status).toBe(200);
  // You cannot directly use response.message, you must use response.body.message.
  expect(response.body.message).toEqual(dirTest);

});

test('returns a JSON message2', async () => {
  const response = await request.get('/crud24/testCRUD?name=' + username + '&surname=' + username + 'surname123');
  expect(response.status).toBe(200);
  expect(response.text).toEqual('{"message":"/crud24/testCRUD?name=' + username + '&surname=' + username + 'surname123 GET REQUEST"}');
});

test('insert formdata record', async () => {
  // generate a unique name and surname conbimation
  // make use of the current timestamp to do this

  let name = username + Date.now();
  let surname = username + 'surname' + Date.now();
  let latitude = 10;
  let longitude = -10;
  let payload = "name=" + name + "&surname=" + surname + "&latitude=" + latitude + "&longitude=" + longitude;
  const response = await request
    .post('/crud24/insertTestFormData')
    .send(payload);

  // response status 200 shows that there have been no code 
  // errors but doesn't verify that the data is in the database
  expect(response.status).toBe(200);


  // now we need to query the geoJSON to see if the data was inserted
  const response1 = await request
    .get('/geoJSON24/getGeoJSON/cege0043/formdata/id/location');
  expect(response1.status).toBe(200);
  expect(response1.text.indexOf('"type":"Feature","properties":{"name":"' + name + '","surname":"' + surname) > 0);

});

test('insert formdata record deliberate duplicate', async () => {
  // generate a unique name and surname conbimation
  // make use of the current timestamp to do this

  let name = username+ Date.now();
  let surname = username+'surname'+Date.now();
  let latitude=20;
  let longitude=-20;
  let payload = "name="+name+"&surname="+surname+"&latitude="+latitude+"&longitude="+longitude;
  const response = await request
        .post('/crud24/insertTestFormData')
        .send(payload);
  expect(response.status).toBe(200);

  // reinsert the identical data
  const response1 = await request
        .post('/crud24/insertTestFormData')
        .send(payload);
  // 500 indicates error, this is set on res.send
  expect(response1.status).toBe(500);

});

test('insert formdata record missing data', async () => {
  // generate a unique name and surname conbimation
  // make use of the current timestamp to do this

  let name = username+ Date.now();
  let latitude=20;
  let longitude=-20;
  let payload = "name="+name+"&latitude="+latitude+"&longitude="+longitude;
  const response = await request
        .post('/crud24/insertTestFormData')
        .send(payload);
  expect(response.status).toBe(500);
  // now we need to query the geoJSON to see if the data was inserted
  const response1 = await request
        .get('/geoJSON24/getGeoJSON/cege0043/formdata/id/location');
  expect(response1.status).toBe(200);
  // we should not be able to find any records with this name
  expect(response1.text.indexOf('"type":"Feature","properties":{"name":"'+name) < 0);

});

test('insert formdata overlong string', async () => {
  // generate a unique name and surname conbimation
  // make use of the current timestamp to do this

  let name = username+ Date.now();
  let surname= username+'surname418'+ Date.now();
  let latitude=15;
  let longitude=-51;
  let lecturetime = '12345678901234';
  // first try a standard insert that should work
  let payload = "name="+name+"&surname="+surname+"&latitude="+latitude+"&longitude="+longitude+"&lecturetime="+lecturetime;
  const response = await request
        .post('/crud24/insertTestFormData')
        .send(payload);
  expect(response.status).toBe(200);

  // now try with an extralong string
  lecturetime = '12345678901234567890';
  payload = "name="+name+"&surname="&surname+"&latitude="+latitude+"&longitude="+longitude+"&lecturetime="&lecturetime;
  const response1 = await request
        .post('/crud24/insertTestFormData')
        .send(payload);
  expect(response1.status).toBe(500);
});