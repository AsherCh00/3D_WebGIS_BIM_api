'use strict';
const supertest = require('supertest');
const request = supertest('http://localhost:4480'); 

test('returns a welcome message', async () => {
    const response = await request.get('/');
    expect(response.status).toBe(200);
    expect(response.text).toEqual("hello world from the Data API on port: 4480");
  });


test('reverses the text', async () => {
    const reversedText = "tset ,olleH";
    
    const response = await request.get('/reverseText?texttoreverse=Hello, test');
    
    expect(response.status).toBe(200);
    expect(response.text).toEqual(reversedText);
  });

