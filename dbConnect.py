import psycopg2
#File which return database connector when called
def dbConnect():

  hostname='localhost'               
  #hardcording database connection parameters  
  
  database='Restaurant'              
  #Database name
  
  username='postgres'                
  #You're username here
  
  pwd='********'                  
  #You're password here
  
  port_id=5432
  #db port
  conn=None
  try:
    conn=psycopg2.connect(           #We use the psycopg2 module to provide the connection
                host=hostname,
                dbname=database,
                user=username,
                password=pwd,
                port=port_id
    )
  except Exception as error:
    print(error)
  return conn                        #Return the connector