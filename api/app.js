'use strict';

const fs = require('fs');
const readline = require('readline');
const elasticsearch = require('elasticsearch');
const client = new elasticsearch.Client({
  host: 'localhost:9200',
  log: 'trace'
});

var indexDocuments = function(val) {
  const str = "../datasets/";
  let dir = `${str}${val}-df/`
  
  fs.readdir(dir, (err, filenames) => {
    if (err) {
      console.log("error stack: " + err);
      throw err;
    } else {
      // returns a list of filenames b.c. spark will often break files up into pieces
      filenames
        .filter(name => name.endsWith(".json"))
        .forEach((filename) => {
          function startReadStream() {
            const instream = fs.createReadStream(dir + filename);
            const outstream = new (require('stream'))();
            const rl = readline.createInterface(instream, outstream);

            rl.on('line', (line) => {
              async function indexDocs() {
                const response = await client.index({
                  index: `yelp-${val}`,
                  body: line
                });

                console.log("Finished Indexing");
              }

              indexDocs(line);
            });

            rl.on('close', (line) => {
              console.log(line);
              console.log('done reading file.');
            });
          }

          setTimeout(startReadStream, 5000);
        });
    }
  });
};

const dirNames = [
  //'us',
  'can'
];

dirNames.forEach((val) => {
  indexDocuments(val);
});
