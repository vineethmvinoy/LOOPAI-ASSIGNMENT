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
#Hard Coding the MaxTime in the database to simulate time of report call

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
    #initiating connection to database with hardcoded values

    curr=conn.cursor()
    #cursor to access rows in tables withing the database

    curr.execute('select distinct(store_id) from store_status')
    #Query to find all the distinct store ids withing the store_status table consisting of all the active and inactive timestamps

    store_ids=curr.fetchall()
    act_count=0
    for row in store_ids: 
    #Loop to iterate through all the distinct store ids and find uptime and downtime off all stores  
     
     timeTotal=0  
     #Variable which counts the total downtime for a store
     
     if(act_count==30): 
       break 
     act_count+=1
     #condition to only show the first 30 store data to prevent long execution/Remove this condition to find all the stores report
     curr.execute(f'select * from store_status where store_id={row[0]} order by store_status.timestamp_utc desc')
     #Query to find all the timestamp of a store in descending order to iterate from closing hour to starting hour
     store_status=curr.fetchall()
     for tRow in store_status:
       tempRow=tRow
       break
     #A Loop that runs one time to find the first row and assign it as tempRow as tempRow is used to calculate timedifference from the localtime 
     count=curr.rowcount #Getting the total number of rows to see if its the last row or first row
     curr.execute(f'select * from store_timezone where store_id={row[0]}')
     #Query to find the timezone of a store using its store id from store_timezone table
     if(curr.rowcount==0):
       store_timezone='America/Chicago'         #if no timezones are found from the above query the timezone is set as 'America/Chicago'
       local_tz=pytz.timezone(store_timezone) 
     else:
      store_timezone=curr.fetchone()
      local_tz=pytz.timezone(store_timezone[1]) #Assigning Timezones
     downtime_last_hour=0 #Variable assigned to 0 before each timestamp is calculated
     downtime_last_day=0  #Only Used downtime because uptime=workinghours-uptime
     downtime_last_week=0
     tempMax=max_Datetime
      #tempMax is initially assigned as the maxtimestamp datetime value,later 
     #in the inner loop it is assigned to the datetime value of the tempRow tuple
     
     tempMax=tempMax.replace(tzinfo=pytz.UTC) 
     tempMax=tempMax.astimezone(local_tz) 
     #Here tempMax that is the max datetime is converted to the localtime of that store
     
     secondarytempMax=tempMax 
     #storing converted max_timestamp to later compare if an hour,day, or a week has passed
     
     tcount=0 
     #count variable to keep track of the number of rows and to see if it is first or last row
     
     for row2 in store_status:   
     #Inner Loop to iterate through all the active/inactive timestamps from store_status table
        
        utcTime=row2[2]
        utcTime=utcTime.replace(tzinfo=pytz.UTC)
        localTime=utcTime.astimezone(local_tz)
        #Getting the locatime as a UTC timestamp and later converting it into local timezone 

        wkDay=localTime.weekday()
        #getting the day from the localtime datetime object to find the day between sunday and saturday assigned with number from 0-6 monday-0, #sunday-6
        
        curr.execute(f'select * from menu_hours where store_id={row[0]} and day={wkDay}')
        #Query to get starting and closing hour for a store on a specific day
        
        if curr.rowcount==0:
        #we assume the store to run 24*7 if the result of the above query return null
          
          temp_Timestamp=datetime(localTime.year,localTime.month,localTime.day)
          startHr=datetime.strptime("0:0:0.0","%H:%M:%S.%f")
          startHr=startHr.time()
          startHr=datetime.combine(temp_Timestamp,startHr)
          closHr=datetime.strptime("23:59:59.99","%H:%M:%S.%f")
          closHr=closHr.time()
          closHr=datetime.combine(temp_Timestamp,closHr)
          #combine the closHr to be datetime object rather than time_ object
        
        elif curr.rowcount==1:
          #same as above we get the closing and starting hour so we assign that and convert it to a datetime object with the localtime day,month&year to be able to compare easily

          storeHours=curr.fetchone()
          startHr=storeHours[2]
          closHr=storeHours[3]
          temp_Timestamp=datetime(localTime.year,localTime.month,localTime.day)
          closHr=datetime.combine(temp_Timestamp,closHr)
          startHr=datetime.combine(temp_Timestamp,startHr) 
        elif(curr.rowcount==2):
        #if there are more than business hours for a store on a specific day the later one is chosen
          thour=curr.fetchall()
          for storeHours in thour:
            startHr=storeHours[2]
            closHr=storeHours[3]
          temp_Timestamp=datetime(localTime.year,localTime.month,localTime.day)
          closHr=datetime.combine(temp_Timestamp,closHr)
          startHr=datetime.combine(temp_Timestamp,startHr)
        
        timeDifference=0
        #assigning the timedifference between two timestamp to be initially zero
        
        if(localTime.timestamp()<startHr.timestamp()):
          continue
        #As we Only have to find downtime during business hours any timestamp after the business hours is not dealt with
        
        if(localTime.timestamp()>closHr.timestamp()):
          tempMax=localTime
          tempRow=row2
          continue
        #If the locatime is greater than the closing time we keep max time to be closing time 

        if(row2[1].strip()=='active' and tempRow[1].strip()=='active'):
          tempMax=localTime  
          tempRow=row2
          continue
        #if we find the current row of timestamp and the previous row have an active status then we avoid the row while counting downtime

        if(tcount==count):
          if(row2[1].strip=='inactive'):
            timeDifference=tempMax.timestamp()-startHr.timestamp()
        #if its the last row and the status is inactive then the difference will be previousRow_timestamp-StartingHour_timestamp
        
        if(tempMax.timestamp()>=closHr.timestamp()):
          tempMax=closHr
          if(tempRow[1].strip()=='inactive'):
            timeDifference=tempMax.timestamp()-localTime.timestamp()
        #if the tempMax is greater than closing hour but current row has status inactive we calculate difference from closing hour
        
        elif(row2[1].strip()=='inactive' or tempRow[1].strip()=='inactive'):
          timeDifference=tempMax.timestamp()-localTime.timestamp()
        #if either the previous or current row has status as inactive we compute difference between their timestamps

        timeTotal=timeTotal+timeDifference
        #timeTotal contains all the downtime hours     
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

