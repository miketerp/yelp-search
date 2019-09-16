const fs = require('fs');
const readline = require('readline');
const elasticsearch = require('elasticsearch');
const client = new elasticsearch.Client({
  host: 'localhost:9200',
  log: 'trace'
});


let dir = '../datasets/modded-df/';
fs.readdir(dir, (err, filenames) => {
  if (err) {
    console.log("error stack: " + err);
    return;
  }
  
  filenames
    .filter(name => name.endsWith(".json"))
    .forEach((filename) => {
      const instream = fs.createReadStream(dir + filename);
      const outstream = new (require('stream'))();
      const rl = readline.createInterface(instream, outstream);

      rl.on('line', (line) => {
        console.log("----------------------------------------------");

        async function indexDocs() {
          const response = await client.index({
            index: 'yelp',
            body: line
          });

          console.log("Finished Indexing");
        }

        indexDocs(line);
        console.log("----------------------------------------------");
      });

      rl.on('close', (line) => {
        console.log(line);
        console.log('done reading file.');
      });
      
      // bulk import is possible too, but need to increase number of files
      // in spark when saving df
      /*
      fs.readFile(dir + filename, 'utf-8', (err, content) => {
        if (err) {
          console.log("error stack: " + err);
          return;
        }
        // do stuff here
        console.log(content);
      });
      */
    });
});

