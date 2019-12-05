'use strict';

const fs = require('fs');
const readline = require('readline');
const redis = require('redis');
const elasticsearch = require('elasticsearch');

const redisClient = redis.createClient('6379','127.0.0.1');
const client = new elasticsearch.Client({
  host: 'localhost:9200',
  log: 'trace'
});

let counter = 1;

redisClient.on('connect', () => {
  console.log("redis client connected");
});

redisClient.on('error', (err) => {
  console.log('Something went wrong ' + err);
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
              //async function indexDocs() {
	      function indexDocs() {
                /*
                const response = await client.index({
                  index: `yelp-${val}`, body: line
                });
                */
		redisClient.set(`ES-key-${counter}`, line, 'EX', 1800);
		counter++;
		console.log("Finished Indexing");
              }

              indexDocs(line);
            });

            rl.on('close', () => console.log('done reading file...'));
          }

          setTimeout(startReadStream, 5000);
        });
    }
  });
};

[ 'us', 'can' ].forEach((val) => {
  indexDocuments(val);
});
