import express, { Request, Response } from 'express';
import { MongoClient, Collection } from 'mongodb';
import { createRequire } from 'module';

const require = createRequire(import.meta.url);
const ipaddr = require('ipaddr.js');

const app = express();
const port = 3000;
const uri = 'mongodb://127.0.0.1:27017';
const client = new MongoClient(uri);

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

let collection: Collection<IPRecord>;

/**
 * Convierte IP a String BigInt exacto para búsqueda por rango
 */
function getIpBigIntString(ip: string): string {
  const addr = ipaddr.parse(ip);
  const parts: number[] = addr.toByteArray();
  let res: bigint = 0n;
  for (const part of parts) {
    res = (res << 8n) + BigInt(part);
  }
  return res.toString();
}

async function start() {
  try {
    await client.connect();
    collection = client.db('ipdb').collection<IPRecord>('ips');
    console.log('📦 Servidor conectado a la base de datos verificada');

    app.listen(port, () => {
      console.log(`🚀 API ipdb corriendo en http://localhost:${port}`);
    });
  } catch (err) {
    console.error('Error inicializando servidor:', err);
  }
}



app.get('/locate/:ip', async (req: Request, res: Response): Promise<void> => {
  try {
    const { ip } = req.params;

    // Type guard para asegurar que es string
    if (typeof ip !== 'string') {
      res.status(400).json({ error: 'Formato de parámetro inválido' });
      return;
    }

    const ipIntString = getIpBigIntString(ip);

    // Búsqueda por rango usando los Strings numéricos
    const result = await collection.findOne({
      start_ip_int: { $lte: ipIntString },
      end_ip_int: { $gte: ipIntString }
    }, { 
      sort: { start_ip_int: -1 }, // Optimización para el índice
      projection: { _id: 0 } 
    });

    if (!result) {
      res.status(404).json({ ip, message: 'IP no encontrada en los rangos actuales' });
      return;
    }

    res.json(result);

  } catch (error) {
    res.status(400).json({ error: 'IP inválida o malformada' });
  }
});

start();