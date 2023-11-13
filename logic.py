from dbConnect import dbConnect
from find_tz import find_tz
from datetime import datetime,tzinfo
from pytz import timezone
import pytz
from find_Times import find_Times
import psycopg2

triggerTime=datetime.strptime("2023-01-25 18:13:22.47922","%Y-%m-%d %H:%M:%S.%f")
#HardCoding the MaxTime in the database to simulate time of report call

finaldata=[]
def logic(report_id,limit):
  try:
    conn=dbConnect()
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
        
        if(act_count==limit): 
          break 
        act_count+=1
        #condition to only show the first 30 store data to prevent long execution/Remove this condition to find all the stores report
        curr.execute(f'select * from store_status where store_id={row[0]} order by store_status.timestamp_utc desc')
        #Query to find all the timestamp of a store in descending order to iterate from closing hour to starting hour
        store_status=curr.fetchall()
        for Row_Iterator in store_status:
          tempRow=Row_Iterator
          break
        #A Loop that runs one time to find the first row and assign it as tempRow as tempRow is used to calculate timedifference from the localtime 
        count=curr.rowcount #Getting the total number of rows to see if its the last row or first row
        
        local_Timezone=find_tz(curr,row[0])
        downtime_last_hour=0 #Variable assigned to 0 before each timestamp is calculated
        downtime_last_day=0  #Only Used downtime because uptime=workinghours-uptime
        downtime_last_week=0
        prev_MaxTime=triggerTime
          #prev_MaxTime is initially assigned as the maxtimestamp datetime value,later 
        #in the inner loop it is assigned to the datetime value of the tempRow tuple
        
        prev_MaxTime=prev_MaxTime.replace(tzinfo=pytz.UTC) 
        prev_MaxTime=prev_MaxTime.astimezone(local_Timezone) 
        #Here prev_MaxTime that is the max datetime is converted to the localtime of that store
        
        secondaryprev_MaxTime=prev_MaxTime 
        #storing converted max_timestamp to later compare if an hour,day, or a week has passed
        
        tcount=0 
        #count variable to keep track of the number of rows and to see if it is first or last row
        for row2 in store_status:   
        #Inner Loop to iterate through all the active/inactive timestamps from store_status table
            
            
            timeDifference=0
            #assigning the timedifference between two timestamp to be initially zero
            localTime,startHr,closHr=find_Times(curr,row2,local_Timezone)
            if(localTime.timestamp()<startHr.timestamp()):
              continue
            #As we Only have to find downtime during business hours any timestamp after the business hours is not dealt with
            
            if(localTime.timestamp()>closHr.timestamp()):
              prev_MaxTime=localTime
              tempRow=row2
              continue
            #If the locatime is greater than the closing time we keep max time to be closing time 

            if(row2[1].strip()=='active' and tempRow[1].strip()=='active'):
              prev_MaxTime=localTime  
              tempRow=row2
              continue
            #if we find the current row of timestamp and the previous row have an active status then we avoid the row while counting downtime

            if(tcount==count):
              if(row2[1].strip=='inactive'):
                timeDifference=prev_MaxTime.timestamp()-startHr.timestamp()
            #if its the last row and the status is inactive then the difference will be previousRow_timestamp-StartingHour_timestamp
            
            if(prev_MaxTime.timestamp()>=closHr.timestamp()):
              prev_MaxTime=closHr
              if(tempRow[1].strip()=='inactive'):
                timeDifference=prev_MaxTime.timestamp()-localTime.timestamp()
            #if the prev_MaxTime is greater than closing hour but current row has status inactive we calculate difference from closing hour
            
            elif(row2[1].strip()=='inactive' or tempRow[1].strip()=='inactive'):
              timeDifference=prev_MaxTime.timestamp()-localTime.timestamp()
            #if either the previous or current row has status as inactive we compute difference between their timestamps

            timeTotal=timeTotal+timeDifference
            #timeTotal contains all the downtime hours     
            absolute_TimeDifference=secondaryprev_MaxTime.timestamp()-localTime.timestamp()
            if(absolute_TimeDifference<=3600):
                if(timeTotal>3600):
                  downtime_last_hour=3600
                else:
                  downtime_last_hour=timeTotal
            if(absolute_TimeDifference<=86400):
                if(timeTotal>86400):
                  downtime_last_day=86400
                else:
                  downtime_last_day=timeTotal
            if(absolute_TimeDifference<=604800):
                if(timeTotal>=604800):
                  downtime_last_week=604800
                else:
                  downtime_last_week=timeTotal       
            prev_MaxTime=localTime  
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
        uptime_last_hour=round(60-downtime_last_hour/60,1)
        uptime_last_day= round(abs(closHr.timestamp()/3600-startHr.timestamp()/3600)-downtime_last_day/3600,1)
        uptime_last_week=round(abs(closHr.timestamp()/3600-startHr.timestamp()/3600)*7-downtime_last_week/3600,1)
        downtime_last_hour=round(downtime_last_hour/60,1)
        downtime_last_day=round(downtime_last_day/3600,1)
        downtime_last_week=round(downtime_last_week/3600,1)
        store_id=row[0]
        curr.execute('INSERT INTO public."REPORTS" VALUES(%s,%s,%s,%s,%s,%s,%s,%s)',(report_id,store_id,uptime_last_hour,uptime_last_day,uptime_last_week,downtime_last_hour,downtime_last_day,downtime_last_week)) 
    curr.execute('UPDATE public."REPORT_MAPPING" SET status=%s where report_id=%s',('COMPLETED',report_id))  
    conn.commit()
    
  except Exception as error:
    print(error) 
  finally:
    if(curr is not None):
      curr.close()
    if(conn is not None):
      conn.close()
