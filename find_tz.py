import pytz
from pytz import timezone

def find_tz(curr,row):
  curr.execute(f'select * from store_timezone where store_id={row}')
  #Query to find the timezone of a store using its store id from store_timezone table

  if(curr.rowcount==0):
        store_timezone='America/Chicago'         #if no timezones are found from the above query the timezone is set as 'America/Chicago'
        local_tz=pytz.timezone(store_timezone) 
  else:
        store_timezone=curr.fetchone()
        local_tz=pytz.timezone(store_timezone[1]) #Assigning Timezones
  return local_tz