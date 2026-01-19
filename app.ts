import ipaddr from 'ipaddr.js'; // FIX: Changed to * as ipaddr
import { MongoClient } from 'mongodb';
import express, { Request, Response } from 'express';

const app = express();
const MONGO_URI = 'mongodb://127.0.0.1:27017';
const client = new MongoClient(MONGO_URI);

try {
  await client.connect();
} catch {
  console.error('> Couldnt connect to the database <');
  process.exit(1);
}

//? Simple interface and creation of types
interface IPRecord {
  start_ip: string;
  end_ip: string;
  start_ip_int: string;
  end_ip_int: string;
  country: string;
  country_name: string;
  continent_name: string;
  ip_version: number;
}
let collection = client.db('ipdb').collection<IPRecord>('ips');

//! Helper function
//? Convert the string to a readable IP
function getIpBigIntString(ip: string): string {
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

app.get('/locate/:ip', async (req: Request, res: Response): Promise<void> => {
  try {
    const ip = Array.isArray(req.params.ip) ? req.params.ip[0] : req.params.ip;

    if (!ipaddr.isValid(ip)) {
      res.status(400).json({ error: 'Invalid IP format' });
      return;
    }

    const ipIntString = getIpBigIntString(ip);

    //? Search the ip for generic range
    const result = await collection.findOne({
      start_ip_int: { $lte: ipIntString },
      end_ip_int: { $gte: ipIntString }
    }, {
      sort: { start_ip_int: -1 }, //! Important for quick response
      projection: { _id: 0 }
    });

    if (!result) throw Error('Auto response catcher');

    res.json(result);
  } catch (error) {
    res.status(404).json({ error: 'IP not found' });
  }
});

export default app;