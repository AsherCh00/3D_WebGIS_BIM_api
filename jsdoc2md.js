'use strict'
const jsdoc2md = require('jsdoc-to-markdown')
const fs = require('fs')
const path = require('path')

/* input and output paths */
//const inputFile = 'example.js'
//const outputDir = __dirname

/* get template data */
//const templateData = jsdoc2md.getTemplateDataSync({ files: inputFile })

/* reduce templateData to an array of class names */
//const classNames = templateData.reduce((classNames, identifier) => {
//  if (identifier.kind === 'class') classNames.push(identifier.name)
//  return classNames
//}, [])

const docs = jsdoc2md.renderSync({ files: 'js/*.js' }).toString();
console.log(docs);

// now put the output into the documentaiton.md file

const timestamp = new Date();
fs.writeFileSync('./documentation.md' ,docs + " <br /> <br />Documentation Updated At: "+timestamp.toString(),{flag:'w'});

/*const fileNames=[];
let files = fs.readdirSync('/data/logbackups/'); //, (err, files) => {
    files.forEach(file => {
        if  (file.indexOf('.js') > -1) {
          fileNames.push('/data/logbackups/'+file.toString());
          console.log("filename is "+ fileNames[totalFiles]);
        } // end if
      });
let documentationMD="";  
for (let i = 0;i<fileNames.lenght -1; i++ ){
      processFile(fileNames[fileNames.length -1]);

}*/

/* create a documentation file for each class */
/*for (const className of classNames) {
  const template = `{{#class name="${className}"}}{{>docs}}{{/class}}`
  console.log(`rendering ${className}, template: ${template}`)
  const output = jsdoc2md.renderSync({ data: templateData, template: template })
  fs.writeFileSync(path.resolve(outputDir, `${className}.md`), output)
}*/