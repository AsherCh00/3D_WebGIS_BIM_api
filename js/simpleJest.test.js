"use strict";
test("string matches",() => {

	// test for match the string - Success
	let string1 = "First test for cege0043"
	expect(string1).toMatch(/cege0043/);

	// test for not match the string - this test will also
	// pass as not.toMatch is correct in this case
	expect(string1).not.toMatch(/abc/)


})

test("string does not match",() => {

	// test for match the string - Success
	let string2 = "Second test for cege0043"
	expect(string2).toMatch(/Second/);


})