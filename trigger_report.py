from logic import logic
from dbConnect import dbConnect
import threading
import random
import string

def generate_report(count):
  
  report_id=''.join(random.choices(string.ascii_uppercase +
                             string.digits, k=7))
  report_id=str(report_id)
  #Random report ID generator to access the created reports later
  
  thread=threading.Thread(target=trigger_report,args=(report_id,count,))
  thread.start()
  #threading the logic program to display status running
  
  try:
    conn=dbConnect()
    curr=conn.cursor()
    curr.execute('INSERT INTO public."REPORT_MAPPING" VALUES(%s,%s)',(report_id,'RUNNING'))
    conn.commit()
    
    #inserting the new report id into the table and setting status as running until completed
    
  except Exception as error:
    print(error)
  finally:
    if(curr is not None):
      curr.close()
    if(conn is not None):
       conn.close()
    return report_id

def trigger_report(report_id,count):
    logic(report_id,count)