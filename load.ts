import fs from 'fs';
import readline from 'readline';
import ipaddr from 'ipaddr.js';
import { MongoClient } from 'mongodb';

//? Constant Important ~Shit~
const URI_MONGO = 'mongodb://127.0.0.1:27017';
const DB_NAME = 'ipdb';
const DB_COLLECTION = 'ips';
const RAWJSON_PATH = 'ipsv1.jsonl';

const client = new MongoClient(URI_MONGO);

//? Convert the string to a readable IP
function toPrecisionString(ip: string): string {
  let addr = ipaddr.parse(ip);

  if (addr.kind() === 'ipv4') {
    addr = (addr as ipaddr.IPv4).toIPv4MappedAddress();
  }

  const parts: number[] = addr.toByteArray();
  let res: bigint = 0n;
  for (const part of parts)
    res = (res << 8n) + BigInt(part);

  return res.toString().padStart(39, '0');
}

//! Main function runner
//? Split the all big file in little chunks to not overload the ram
async function run() {
  await client.connect();
  const db = client.db(DB_NAME);
  const collection = db.collection(DB_COLLECTION);

  //! Cleaning the db (optional)
  console.log('> Cleaning the database for a new one');
  await collection.deleteMany({});

  const fileStream = fs.createReadStream(RAWJSON_PATH);
  const rl = readline.createInterface({ input: fileStream, crlfDelay: Infinity });

  let batch: any[] = [];
  const BATCH_SIZE = 10000; //! Size of the chunk
  let total = 0;

  console.time('> Loading finished in ');

  for await (const line of rl) {
    const trimmed = line.trim();
    if (!trimmed || !trimmed.startsWith('{')) {
      console.log(`> Skipping non-JSON line: ${trimmed.slice(0, 20)}...`);
      continue;
    }

    const raw = JSON.parse(line);

    //? The data is shit so remove the _id from the json to create a new one
    delete raw._id;

    //? Load again the data to big int notation to be more precise
    try {
      raw.start_ip_int = toPrecisionString(raw.start_ip);
      raw.end_ip_int = toPrecisionString(raw.end_ip);
    } catch (e) {
      console.log(`> Error parsing IP: ${raw.start_ip}`);
      continue;
    }

    //? Remove all the no data objects and put it as common names
    if (raw.asn && typeof raw.asn === 'object') raw.asn = null;
    if (raw.as_name && typeof raw.as_name === 'object') raw.as_name = "Unknown";

    //? Push to the chunk
    batch.push({ insertOne: { document: raw } });

    if (batch.length >= BATCH_SIZE) {
      await collection.bulkWrite(batch, { ordered: false });
      total += batch.length;
      process.stdout.write(`\r Packages loaded: ${total.toLocaleString()}`);
      batch = [];
    }
  }

  if (batch.length > 0) {
    await collection.bulkWrite(batch, { ordered: false });
    total += batch.length;
  }

  console.log('\n');
  console.timeEnd('> Loading finished in ');

  //! Creation of idxs to improve time of request of the data
  console.log('> Creating idxs');
  await collection.createIndex({ start_ip_int: 1, end_ip_int: 1 });

  console.log('> Database loaded and checked <');
}

(async () => {
  try {
    await run();
  } catch (err) {
    console.log('Catch error: ', err);
  } finally {
    await client.close();
  }
})();