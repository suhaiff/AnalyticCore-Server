const { MongoClient } = require('mongodb');
const uri = "mongodb+srv://sohibvtab_db_user:Iz4I6IyIqHTY6IUQ@cluster0.lnkuyyc.mongodb.net/?appName=Cluster0";
async function run() {
  try {
    const client = new MongoClient(uri);
    await client.connect();
    console.log("Connected successfully to server");
    await client.close();
  } catch (err) {
    console.error("Connection error:", err);
  }
}
run();
