from logic import logic
from dbConnect import dbConnect
import threading
import random
import string

def generate_report(count):
  report_id=''.join(random.choices(string.ascii_uppercase +
                             string.digits, k=7))
  report_id=str(report_id)
  thread=threading.Thread(target=trigger_report,args=(report_id,count,))
  thread.start()
  try:
    conn=dbConnect()
    curr=conn.cursor()
    curr.execute('INSERT INTO public."REPORT_MAPPING" VALUES(%s,%s)',(report_id,'RUNNING'))
    conn.commit()
    
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