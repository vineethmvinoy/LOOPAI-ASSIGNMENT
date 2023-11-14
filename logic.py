from dbConnect import dbConnect
from find_tz import find_tz
from datetime import datetime,timedelta
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
          prev_StatusRow=Row_Iterator
          break
        #A Loop that runs one time to find the first row and assign it as prev_StatusRow as prev_StatusRow is used to calculate timedifference from the localtime 
        count=curr.rowcount #Getting the total number of rows to see if its the last row or first row
        
        local_Timezone=find_tz(curr,row[0])
        downtime_last_hour=0 #Variable assigned to 0 before each timestamp is calculated
        downtime_last_day=0  #Only Used downtime because uptime=workinghours-uptime
        downtime_last_week=0
        prev_MaxTime=triggerTime
          #prev_MaxTime is initially assigned as the maxtimestamp datetime value,later 
        #in the inner loop it is assigned to the datetime value of the prev_StatusRow tuple
        
        prev_MaxTime=prev_MaxTime.replace(tzinfo=pytz.UTC) 
        prev_MaxTime=prev_MaxTime.astimezone(local_Timezone) 
        #Here prev_MaxTime that is the max datetime is converted to the localtime of that store
        
        secondaryprev_MaxTime=prev_MaxTime 
        #storing converted max_timestamp to later compare if an hour,day, or a week has passed
        
        tcount=0 
        #count variable to keep track of the number of rows and to see if it is first or last row
        Hour_complete=False
        Day_complete=False
        Week_complete=False
        #Variable to keep track if an hour,day or week has passed

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
              prev_StatusRow=row2
              continue
            #If the locatime is greater than the closing time we keep max time to be closing time 

            if(row2[1].strip()=='active' and prev_StatusRow[1].strip()=='active'):
              prev_MaxTime=localTime  
              prev_StatusRow=row2
              continue
            #if we find the current row of timestamp and the previous row have an active status then we avoid the row while counting downtime

            if(tcount==count):
              if(row2[1].strip=='inactive'):
                timeDifference=prev_MaxTime.timestamp()-startHr.timestamp()
            #if its the last row and the status is inactive then the difference will be previousRow_timestamp-StartingHour_timestamp
            
            if(prev_MaxTime.timestamp()>=closHr.timestamp()):
              prev_MaxTime=closHr
              if(prev_StatusRow[1].strip()=='inactive'):
                timeDifference=prev_MaxTime.timestamp()-localTime.timestamp()
            #if the prev_MaxTime is greater than closing hour but current row has status inactive we calculate difference from closing hour
            
            elif(row2[1].strip()=='inactive' or prev_StatusRow[1].strip()=='inactive'):
              timeDifference=prev_MaxTime.timestamp()-localTime.timestamp()
            #if either the previous or current row has status as inactive we compute difference between their timestamps

            timeTotal=timeTotal+timeDifference
            #triggerTimeToLocalTimeDifference is a variable used to keep track of the time passed while iterating backwards to check if we passed an hour,day or a week
            OneHrBeforeTriggerTime=secondaryprev_MaxTime-timedelta(hours=1)
            #We are storing a time that is one hr less than the trigger time to check if timestamps of stores are within onehr range

            #so Basically we are iterating through the store_status table with pointing to two rows the current timestamp and the one before it which is either the trigger time or clostime whichever is smaller
            if(Hour_complete==False):
              #Condition if hourly downtime has been set or not

              if(localTime.timestamp()>OneHrBeforeTriggerTime.timestamp() and prev_MaxTime.timestamp()>secondaryprev_MaxTime.timestamp() and localTime.timestamp()<secondaryprev_MaxTime.timestamp()):
              #Checking to see if the current time stamp is within the one hr range and timestamp before it is greater than the trigger time
              
                downtime_last_hour=secondaryprev_MaxTime.timestamp()-localTime.timestamp()
                #if second timestamp greater than trigger time then downtime is triggertime - current timestamp

               #Alert :WE ONLY COMPUTE DOWN TIME and use starting and closing hours to compute uptime
              if(localTime.timestamp()>OneHrBeforeTriggerTime.timestamp() and localTime.timestamp()<secondaryprev_MaxTime.timestamp() and prev_MaxTime.timestamp()<secondaryprev_MaxTime.timestamp() and prev_MaxTime.timestamp()>OneHrBeforeTriggerTime.timestamp()):
                #Checking to see if both current and prev timestamp is within the one hr range 
                
                downtime_last_hour+=prev_MaxTime.timestamp()-localTime.timestamp()
                #adding the downtime between two timestamps to downtime in last hour

              elif(localTime.timestamp()<OneHrBeforeTriggerTime.timestamp() and prev_MaxTime.timestamp()>OneHrBeforeTriggerTime.timestamp()):
                #checking to see if current timestamp is less than the one hr range but previous timestamp is within one hr range
                
                downtime_last_hour+=prev_MaxTime.timestamp()-OneHrBeforeTriggerTime.timestamp()
                #adding the downtime between two timestamps to downtime in last hour to be only within the one hr range
                
                Hour_complete=True
                #as all the time within one hr will be assigned with the above condition hourly downtime doesnt have to be computed anymore    
            OneDayBeforeTriggerTime=secondaryprev_MaxTime-timedelta(days=1)
            #Similar to one hour time frame we calculate the same one day time frame
            if(Day_complete==False):
              if(localTime.timestamp()>OneDayBeforeTriggerTime.timestamp() and prev_MaxTime.timestamp()>secondaryprev_MaxTime.timestamp() and localTime.timestamp()<secondaryprev_MaxTime.timestamp()):
                downtime_last_day=secondaryprev_MaxTime.timestamp()-localTime.timestamp()
              if(localTime.timestamp()>OneDayBeforeTriggerTime.timestamp() and localTime.timestamp()<secondaryprev_MaxTime.timestamp() and prev_MaxTime.timestamp()<secondaryprev_MaxTime.timestamp() and prev_MaxTime.timestamp()>OneDayBeforeTriggerTime.timestamp()):
                downtime_last_day+=prev_MaxTime.timestamp()-localTime.timestamp()
              elif(localTime.timestamp()<OneDayBeforeTriggerTime.timestamp() and prev_MaxTime.timestamp()>OneDayBeforeTriggerTime.timestamp()):
                downtime_last_day+=prev_MaxTime.timestamp()-OneDayBeforeTriggerTime.timestamp()
                Day_complete=True
            OneWeekBeforeTriggerTime=secondaryprev_MaxTime-timedelta(weeks=1)
            #Similar to one hour time frame we calculate the same one week time frame
            if(Week_complete==False):
              if(localTime.timestamp()>OneWeekBeforeTriggerTime.timestamp() and prev_MaxTime.timestamp()>secondaryprev_MaxTime.timestamp() and localTime.timestamp()<secondaryprev_MaxTime.timestamp()):
                downtime_last_week=secondaryprev_MaxTime.timestamp()-localTime.timestamp()
              if(localTime.timestamp()>OneWeekBeforeTriggerTime.timestamp() and localTime.timestamp()<secondaryprev_MaxTime.timestamp() and prev_MaxTime.timestamp()<secondaryprev_MaxTime.timestamp() and prev_MaxTime.timestamp()>OneWeekBeforeTriggerTime.timestamp()):
                downtime_last_week+=prev_MaxTime.timestamp()-localTime.timestamp()
              elif(localTime.timestamp()<OneDayBeforeTriggerTime.timestamp() and prev_MaxTime.timestamp()>OneDayBeforeTriggerTime.timestamp()):
                downtime_last_week+=prev_MaxTime.timestamp()-OneWeekBeforeTriggerTime.timestamp()
                Week_complete=True   
            prev_MaxTime=localTime               
            prev_StatusRow=row2       

        uptime_last_hour=round(60-downtime_last_hour/60,1)
        uptime_last_day= round(abs(closHr.timestamp()/3600-startHr.timestamp()/3600)-downtime_last_day/3600,1)
        uptime_last_week=round(abs(closHr.timestamp()/3600-startHr.timestamp()/3600)*7-downtime_last_week/3600,1)
        #calculate uptime from the remaining time from the business hours-closing time

        downtime_last_hour=round(downtime_last_hour/60,1)
        downtime_last_day=round(downtime_last_day/3600,1)
        downtime_last_week=round(downtime_last_week/3600,1)
        store_id=row[0]

        curr.execute('INSERT INTO public."REPORTS" VALUES(%s,%s,%s,%s,%s,%s,%s,%s)',(report_id,store_id,uptime_last_hour,uptime_last_day,uptime_last_week,downtime_last_hour,downtime_last_day,downtime_last_week)) 
        #Input the store report into the table

    curr.execute('UPDATE public."REPORT_MAPPING" SET status=%s where report_id=%s',('COMPLETED',report_id)) 
    #update the status of the REPORT GENERATION FROM RUNNING TO COMPLETED 
    conn.commit()
    #COMMIT THE CHANGES CREATED TO THE DATABASE
  except Exception as error:
    print(error) 
  finally:
    if(curr is not None):
      curr.close()
    if(conn is not None):
      conn.close()
