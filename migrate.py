import json
import psycopg2
import ipaddress
from psycopg2.extras import execute_values

#? Configuration of the container
DB_USER = 'postgres'
DB_PASS = 'admin'
DB_HOST = 'localhost'
DB_PORT = '5545'
DB_NAME = 'ipdb-dev'


def obtain_clean_value(data, key):
  value = data.get(key)
  if isinstance(value, dict):
    return None
  if value in ('null', 'NaN', 'nan'):
    return None
  return value

def prepare_database(conn):
  with conn.cursor() as cur:
    print('> Creating table...')
    cur.execute('''
      CREATE TABLE IF NOT EXISTS ip_blocks (
        id SERIAL PRIMARY KEY,
        network CIDR NOT NULL,
        country_code CHAR(2),
        country_name VARCHAR(100),
        continent_code CHAR(2),
        continent_name VARCHAR(100)
      );
    ''')
    print('> Integrating index GiST for better searching...')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_ip_blocks_network ON ip_blocks USING gist (network inet_ops);')
  conn.commit()

def process_and_insert(conn, file):
  records = []

  print('> Processing IPs to blocks CIDR...')
  with open(file, 'r') as f:
    for line in f:
      clean_line = line.strip()
      
      if not clean_line or clean_line in ("[", "]"):
        continue
        
      if clean_line.endswith(','):
        clean_line = clean_line[:-1]

      try:
        clean_line = clean_line.replace("NaN", "null")
        data = json.loads(clean_line)

        #? Converting the strings to IP native objectives
        start = ipaddress.ip_address(data['start_ip'])
        end = ipaddress.ip_address(data['end_ip'])

        #? Create the range of CIDR
        cidrs = list(ipaddress.summarize_address_range(start, end))

        for cidr in cidrs:
          records.append((
            str(cidr),
            obtain_clean_value(data, 'country'),
            obtain_clean_value(data, 'country_name'),
            obtain_clean_value(data, 'continent'),
            obtain_clean_value(data, 'continent_name')
          ))
        
        print(f'> Successfull line [{clean_line:15}]')
      except Exception as e:
        print(f'> Breaking line for error: {e} | Line: {clean_line:60}...')
  with conn.cursor() as cur:
    query = '''
      INSERT INTO ip_blocks (network, country_code, country_name, continent_code, continent_name)
      VALUES %s
    '''
    execute_values(cur, query, records)
  conn.commit()
  print('> Migration successfull!')

if __name__ == '__main__':
  conn = psycopg2.connect(
    dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT
  )
  prepare_database(conn)

  process_and_insert(conn, 'ipsv1.json')

  conn.close()