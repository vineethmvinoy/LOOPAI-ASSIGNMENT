import psycopg2
import json
import time
from datetime import datetime,tzinfo
from dateutil import tz
from flask import Flask
from pytz import timezone
import pytz
import threading
finaldata=[]

max_Datetime=datetime.strptime("2023-01-25 18:13:22.47922","%Y-%m-%d %H:%M:%S.%f")


def trigger_report():
  hostname='localhost'
  database='Restaurant'
  username='postgres'
  pwd='lominus@E53'
  port_id=5432
  conn=None
  curr=None
  try:
    conn=psycopg2.connect(
                host=hostname,
                dbname=database,
                user=username,
                password=pwd,
                port=port_id
    )
    curr=conn.cursor()
    curr.execute('select distinct(store_id) from store_status')
    store_ids=curr.fetchall()
    act_count=0
    for row in store_ids:  
     timeTotal=0
     if(act_count==30):
       break
     act_count+=1
     curr.execute(f'select * from store_status where store_id={row[0]} order by store_status.timestamp_utc desc')
     store_status=curr.fetchall()
     for tRow in store_status:
       tempRow=tRow
       break
     count=curr.rowcount
     curr.execute(f'select * from store_timezone where store_id={row[0]}')
     if(curr.rowcount==0):
       store_timezone='America/Chicago'
       local_tz=pytz.timezone(store_timezone)
     else:
      store_timezone=curr.fetchone()
      local_tz=pytz.timezone(store_timezone[1])
     uptime_last_hour=0
     uptime_last_day=0
     uptime_last_week=0
     downtime_last_hour=0
     downtime_last_day=0
     downtime_last_week=0
     tempMax=max_Datetime
     print(tempMax)
     tempMax=tempMax.replace(tzinfo=pytz.UTC)
     tempMax=tempMax.astimezone(local_tz)
     print(tempMax)
     secondarytempMax=tempMax
     tcount=0
     for row2 in store_status:   
        utcTime=row2[2]
        utcTime=utcTime.replace(tzinfo=pytz.UTC)
        localTime=utcTime.astimezone(local_tz)
        wkDay=localTime.weekday()
        curr.execute(f'select * from menu_hours where store_id={row[0]} and day={wkDay}')
        if curr.rowcount==0:
          temp_Timestamp=datetime(localTime.year,localTime.month,localTime.day)
          startHr=datetime.strptime("0:0:0.0","%H:%M:%S.%f")
          startHr=startHr.time()
          startHr=datetime.combine(temp_Timestamp,startHr)
          closHr=datetime.strptime("23:59:59.99","%H:%M:%S.%f")
          closHr=closHr.time()
          closHr=datetime.combine(temp_Timestamp,closHr)
        elif curr.rowcount==1:
          storeHours=curr.fetchone()
          startHr=storeHours[2]
          closHr=storeHours[3]
          temp_Timestamp=datetime(localTime.year,localTime.month,localTime.day)
          closHr=datetime.combine(temp_Timestamp,closHr)
          startHr=datetime.combine(temp_Timestamp,startHr) 
        elif(curr.rowcount==2):
          thour=curr.fetchall()
          for storeHours in thour:
            startHr=storeHours[2]
            closHr=storeHours[3]
          temp_Timestamp=datetime(localTime.year,localTime.month,localTime.day)
          closHr=datetime.combine(temp_Timestamp,closHr)
          startHr=datetime.combine(temp_Timestamp,startHr)
        timeDifference=0
        if(localTime.timestamp()<startHr.timestamp()):
          continue
        if(localTime.timestamp()>closHr.timestamp()):
          tempMax=localTime
          tempRow=row2
          continue
        if(row2[1].strip()=='active' and tempRow[1].strip()=='active'):
          tempMax=localTime  
          tempRow=row2
          continue
        if(tcount==count):
          if(row2[1].strip=='inactive'):
            timeDifference=tempMax.timestamp()-startHr.timestamp()
        if(tempMax.timestamp()>=closHr.timestamp()):
          tempMax=closHr
          if(tempRow[1].strip()=='inactive'):
            timeDifference=tempMax.timestamp()-localTime.timestamp()
        elif(row2[1].strip()=='inactive' or tempRow[1].strip()=='inactive'):
          timeDifference=tempMax.timestamp()-localTime.timestamp()
        timeTotal=timeTotal+timeDifference     
        if(row[0]==924494512866798795):
         print(row[0],closHr,tempMax,localTime,startHr,timeDifference)
        absolute_TimeDifference=secondarytempMax.timestamp()-localTime.timestamp()
        if(absolute_TimeDifference<=3600):
            if(timeTotal>3600):
              downtime_last_hour=3600
            else:
              downtime_last_hour=timeTotal
        elif(absolute_TimeDifference>3600 and (row2[1].strip()=='inactive' and tempRow[1].strip()=='inactive')):
          if(downtime_last_hour+timeDifference>3600):
            downtime_last_hour=3600
          else:
            downtime_last_hour-timeTotal          
        if(absolute_TimeDifference<=86400):
            if(timeTotal>86400):
              downtime_last_day=86400
            else:
              downtime_last_day=timeTotal
        elif(absolute_TimeDifference>86400 and (row2[1].strip()=='inactive' and tempRow[1].strip()=='inactive')):
            if(downtime_last_hour+timeDifference>86400):
              downtime_last_hour=86400
            else:
              downtime_last_hour-timeTotal            
        if(absolute_TimeDifference<=604800):
            if(timeTotal>=604800):
              downtime_last_week=604800
            else:
              downtime_last_week=timeTotal
        elif(absolute_TimeDifference>604800 and (row2[1].strip()=='inactive' and tempRow[1].strip()=='inactive')):
            if(downtime_last_hour+timeDifference>604800):
              downtime_last_hour=604800
            else:
              downtime_last_hour-timeTotal            
        tempMax=localTime  
        tempRow=row2       
     finaldata.append(
                        {
                        'store_id':row[0],
                        'uptime_last_hour':round(60-downtime_last_hour/60,1),
                        'uptime_last_day':round(abs(closHr.timestamp()/3600-startHr.timestamp()/3600)-downtime_last_day/3600,1),
                        'uptime_last_week':round(abs(closHr.timestamp()/3600-startHr.timestamp()/3600)*7-downtime_last_week/3600,1),
                        'downtime_last_hour':round(downtime_last_hour/60,1),
                        'downtime_last_day':round(downtime_last_day/3600,1),
                        'downtime_last_week':round(downtime_last_week/3600,1)
                        }
                     )
  except Exception as error:
    print(error)
  finally:
    if curr is not None:
      curr.close()
    if conn is not None:
      conn.close()
x=threading.Thread(target=trigger_report)  
app=Flask(__name__)
@app.route('/get_report/<int:n>',methods=['GET'])
def get_report(n):
  if (x.is_alive()):
    return {'status':'Running',
            'doneCount':len(finaldata)}
  elif(n==123456789):
    return finaldata
  else:
    return {'status':'Invalid Data'}
@app.route('/trigger_report',methods=['GET'])
def homepage():
  x.start()
  report_id={'reportId':123456789}
  return report_id

if __name__=="__main__":
  app.run(debug=True)

