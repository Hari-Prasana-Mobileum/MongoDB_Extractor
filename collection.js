const mongoose = require("mongoose");
const fs = require("fs");
const { parseAsync } = require("json2csv");

// const User = require("./userModel");

// MongoDB connection URL
const url = "mongodb://localhost:27017/"; // Replace with your MongoDB server URL

// Database and collection name
const dbName = "test"; // Replace with your database name
// const collectionName = "isos"; // Replace with your collection name

// Number of records to fetch per batch
const batchSize = 1000;

let totalRecordsProcessed = 0;

const collectionName = process.argv.slice(2)[0];

console.log(collectionName);

if (collectionName) {
  mongoose
    .connect(url + dbName)
    .then(async () => {
      console.log("connected to db & listening on port");
      const CollectionSchema = new mongoose.Schema({});

      const Collection = mongoose.model(collectionName, CollectionSchema);

      const processBatch = async (skip) => {
        const data = await Collection.find({}).skip(skip).limit(batchSize).lean();
        if (data.length === 0) {
          console.log(
            `Extraction completed. Total records: ${totalRecordsProcessed}`
          );
          return;
        }

        totalRecordsProcessed += data.length;

        const fields = Object.keys(data[0]);

        console.log(fields)

        // Configure the JSON to CSV options
        const json2csvOptions = { fields };

        // Convert the data to CSV format
        const csv = await parseAsync(data, json2csvOptions);

        // const csvData = json2csv(data);
        // console.log(data)
        // console.log(csvData)

        // Write CSV data to the file
        fs.appendFile(
          `${collectionName}-${Date.now()}-${skip / batchSize + 1}.csv`,
          csv,
          (writeErr) => {
            if (writeErr) {
              console.error("Error writing to CSV:", writeErr);
            } else {
              console.log(`Processed ${totalRecordsProcessed} records`);
              processBatch(skip + batchSize);
            }
          }
        );
      };

      processBatch(0);
    })
    .catch((error) => {
      console.log(error);
    });
} else {
  console.log("provide collection name in arguments!");
}
