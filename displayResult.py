from dbConnect import dbConnect

def displayResult(report_id):
  finaldata=[]
  conn=dbConnect()
  curr=conn.cursor()
  curr.execute('select * from public."REPORT_MAPPING" where report_id=%s',(report_id,))
  if(curr.rowcount==0):
    finaldata={'status':'Invalid Report Id'}
  else:
    Report_Status=curr.fetchone()
    if(Report_Status[1].strip()=='RUNNING'):
      finaldata={'report_Id':report_id,'status':'RUNNING'}
    else:
      curr.execute('select * from public."REPORTS" where report_id=%s',(report_id,))
      REPORTS=curr.fetchall()
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
      finaldata={'report_id':report_id,
              'status':'COMPLETED',
              'REPORTS':finaldata}
  return finaldata
# except Exception as error:
#   print(error)
# finally:
#   if(curr is not None):
#     curr.close()
#   if(conn is not None):
#     conn.close()
    
    