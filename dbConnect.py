import psycopg2

def dbConnect():

  hostname='localhost'                 
  database='Restaurant'
  username='postgres'
  pwd='lominus@E53'
  port_id=5432
  conn=None
  try:
    conn=psycopg2.connect(
                host=hostname,
                dbname=database,
                user=username,
                password=pwd,
                port=port_id
    )
  except Exception as error:
    print(error)
  return conn