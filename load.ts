import { MongoClient, Collection } from 'mongodb';
import fs from 'fs';
import readline from 'readline';
import { createRequire } from 'module';

const require = createRequire(import.meta.url);
const ipaddr = require('ipaddr.js');

const URI = 'mongodb://127.0.0.1:27017';
const client = new MongoClient(URI);

/**
 * Convierte IP a String de BigInt con precisión total
 */
function toPrecisionString(ip: string): string {
  const addr = ipaddr.parse(ip);
  const parts: number[] = addr.toByteArray();
  let res: bigint = 0n;
  for (const part of parts) {
    res = (res << 8n) + BigInt(part);
  }
  return res.toString();
}

async function run() {
  try {
    await client.connect();
    const db = client.db('ipdb');
    const collection = db.collection('ips');

    // 1. Limpieza total
    console.log('🧹 Limpiando base de datos para nueva carga...');
    await collection.deleteMany({});
    
    const fileStream = fs.createReadStream('ipsv1.jsonl');
    const rl = readline.createInterface({ input: fileStream, crlfDelay: Infinity });

    let batch: any[] = [];
    const BATCH_SIZE = 10000;
    let total = 0;

    console.time('⏱️ Carga finalizada en');

    for await (const line of rl) {
      if (!line.trim()) continue;

      const raw = JSON.parse(line);
      
      // Eliminamos el _id que traiga el JSON para que Mongo genere uno nuevo limpio
      delete raw._id;

      // FORZAMOS LA PRECISIÓN: Re-calculamos los enteros como Strings
      // Esto ignora lo que traiga el JSON y asegura que no haya notación científica
      raw.start_ip_int = toPrecisionString(raw.start_ip);
      raw.end_ip_int = toPrecisionString(raw.end_ip);

      // Limpieza de campos nulos/NaN que vimos antes
      if (raw.asn && typeof raw.asn === 'object') raw.asn = null;
      if (raw.as_name && typeof raw.as_name === 'object') raw.as_name = "Unknown";

      batch.push({ insertOne: { document: raw } });

      if (batch.length >= BATCH_SIZE) {
        await collection.bulkWrite(batch, { ordered: false });
        total += batch.length;
        process.stdout.write(`\r📦 Registros cargados: ${total.toLocaleString()}`);
        batch = [];
      }
    }

    if (batch.length > 0) {
      await collection.bulkWrite(batch, { ordered: false });
      total += batch.length;
    }

    console.log('\n');
    console.timeEnd('⏱️ Carga finalizada en');

    // 2. CREACIÓN DE ÍNDICES (Indispensable para que la API sea rápida)
    console.log('📂 Creando índices...');
    await collection.createIndex({ start_ip_int: 1, end_ip_int: 1 });
    
    console.log('✅ Base de datos lista y verificada.');

  } catch (err) {
    console.error('🔴 Error:', err);
  } finally {
    await client.close();
  }
}

run();