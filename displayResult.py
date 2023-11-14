from dbConnect import dbConnect

def displayResult(report_id):
  try:
    finaldata=[]

    #temporary placeholder for finaldata to be returned
    
    conn=dbConnect()

    #acquiring db connection
    
    curr=conn.cursor()

    #assigning cursor from db connection

    curr.execute('select * from public."REPORT_MAPPING" where report_id=%s',(report_id,))

    #query to check whether the process has been completed or not using the status attribute

    if(curr.rowcount==0):
      finaldata={'status':'Invalid Report Id'}

    #Condition to check whether the given report id in the url is correct or not

    else:
      Report_Status=curr.fetchone()
      if(Report_Status[1].strip()=='RUNNING'):
        finaldata={'report_Id':report_id,'status':'RUNNING'}

      #if report id is found and status is running return running status

      else:
        curr.execute('select * from public."REPORTS" where report_id=%s',(report_id,))
        REPORTS=curr.fetchall()

        #if returned status as complete assign all the generated values onto a dictionary and return
        
        for row in REPORTS:

          finaldata.append(
                          {
                            'store_id':row[1],
                            'uptime_last_hour':row[2],
                            'uptime_last_day':row[3],
                            'uptime_last_week':row[4],
                            'downtime_last_hour':row[5],
                            'downtime_last_day':row[6],
                            'downtime_last_week':row[7]
                          }
                          )                
        finaldata={'REPORTS':finaldata,
                   'report_id':report_id,
                'status':'COMPLETED',
                }
        
    #assigning the placeholder with required values
    
    return finaldata
    #return the placeholder
  except Exception as error:
    print(error)
  finally:
    if(curr is not None):
      curr.close()
    if(conn is not None):
      conn.close()
    
    