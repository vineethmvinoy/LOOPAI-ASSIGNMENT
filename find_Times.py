from pytz import timezone
import pytz
from datetime import datetime,tzinfo
def find_Times(curr,row,local_tz):
  utcTime=row[2]
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
      #retrieve the local hours
    
    temp_Timestamp=datetime(localTime.year,localTime.month,localTime.day)
    closHr=datetime.combine(temp_Timestamp,closHr)
    startHr=datetime.combine(temp_Timestamp,startHr)
    #Convert the retrieved hours in to a specific to date easier comparison using the local time's date
  return localTime,startHr,closHr