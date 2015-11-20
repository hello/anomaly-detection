import csv
import numpy as np
import pandas as pd

import psycopg2
from sqlalchemy import create_engine
import sys

import datetime
from datetime import date, timedelta

###############################################################################################
# get past 30 days
###############################################################################################
def sql_connect():
    try:
        connection = psycopg2.connect("host=sensors2.cy7n0vzxfedi.us-east-1.redshift.amazonaws.com user=jyfan password=HelloRedshiftSensor1 port=5439 dbname=sensors1")
        return connection
    except:
        print "Unable to connect to database"

def sql_query(conn, account_id, end_query_date, num_days=30, start_hour=0, end_hour=6, hour_bucket=2):
    cur = conn.cursor()
    
    end_query_date = datetime.datetime.strptime(end_query_date, '%Y-%m-%d').date()
    start_query_date = end_query_date - timedelta(days=num_days) 

    query = """SELECT MIN(EXTRACT(month FROM local_utc_ts)) AS month, MIN(EXTRACT(day FROM local_utc_ts)) AS date, MIN(EXTRACT(hour FROM local_utc_ts)) AS hour, 
                AVG(ambient_light) AS avg_light, COUNT(ambient_light) AS count FROM device_sensors_master 
                    WHERE account_id='%d' 
                        AND (local_utc_ts > '%s' AND local_utc_ts <= '%s')
                        AND (EXTRACT(hour FROM local_utc_ts) >= %d AND EXTRACT(hour FROM local_utc_ts) < %d)
                    GROUP BY EXTRACT(day FROM local_utc_ts), (EXTRACT(hour FROM local_utc_ts) / %d)
                    ORDER BY date, hour;""" % (account_id, '{:%Y-%m-%d}'.format(start_query_date), '{:%Y-%m-%d}'.format(end_query_date), start_hour, end_hour, hour_bucket)
#    print query
    try:
        cur.execute(query)
    except psycopg2.Error as error:
        print error.pgcode 
    history = cur.fetchall()

    historyArray = np.asarray(history)
    historyDF = pd.DataFrame(historyArray, columns=['month','day','hour','avg_light','count'])
    return historyDF 

###############################################################################################
# clean 
###############################################################################################
def clean_data(historyDF, hourArray, hour_bucket):

    min_month = min(historyDF['month'])
    max_month = max(historyDF['month'])

    monthArray = np.arange(min_month, max_month + 1)
    for month in monthArray:
        min_date = min(historyDF[historyDF['month']==month]['day'])
        max_date = max(historyDF[historyDF['month']==month]['day'])
        dateArray = np.arange(min_date, max_date + 1)
        for date in dateArray:
            for hour in hourArray: # if month,date,hour DNE or count for month,date,year is less than 120, delete row
                if len(historyDF[historyDF['month']==month][historyDF['day']==date][historyDF['hour']==hour])==0:
                    monthBol = historyDF['month'] == month
                    dayBol = historyDF['day'] == date
                    deleteBol = monthBol & dayBol
                    historyDF = historyDF[~deleteBol]

                elif historyDF[historyDF['month']==month][historyDF['day']==date][historyDF['hour']==hour]['count'].item() < (hour_bucket * 60): 
                    monthBol = historyDF['month'] == month
                    dayBol = historyDF['day'] == date
                    deleteBol = monthBol & dayBol
                    historyDF = historyDF[~deleteBol]

    historyDF_clean = historyDF
    return historyDF_clean 

def format_data(historyDF_clean, hourArray):
    """
    hour          0   2   4
    month day              
    10    2     136  10  10
          3       9  10   9
                ...      
    """
    historyDF_format = historyDF_clean.pivot_table(index=['month','day'], columns='hour', values='avg_light')
    return historyDF_format 

###############################################################################################
# get parameters, write parameters 
###############################################################################################
def calc_params(historyDF_format, account_id, lower_bound, query_end_date, num_days, hourArray, version):
    """
      account_id date_computed lower_bound num_days version med-0 left-0  \
    0          1    2015-11-18          10       30     iqr  11.5  10.45   

        right-0 med-2 left-2 right-2 med-4 left-4 right-4  
    0  15021.95    11     10      12    11   9.45      12  
    """
    left = lower_bound/2.0
    right = 100 - (lower_bound/2.0)

    titleArray = ['account_id','date_computed','query_end_date','lower_bound','num_days','version']
    params = [account_id, date.today(), query_end_date, lower_bound, num_days, version]
    for hour in hourArray:
        medTitle = 'med-' + str(hour)
        leftTitle = 'left-' + str(hour)
        rightTitle = 'right-' + str(hour)       
        titleArray.extend((medTitle, leftTitle, rightTitle))
  
        hourData = np.asarray(historyDF_format[hour].values)
        medParam = np.percentile(hourData,50)
        leftParam = np.percentile(hourData,left)
        rightParam = np.percentile(hourData,right)
        params.extend((medParam, leftParam, rightParam)) 

    params = np.asarray(params)[np.newaxis]
    paramsDF = pd.DataFrame(params, columns=titleArray)
    return paramsDF 

def write_params(paramsDF, db_url, table_name):
    engine = create_engine(db_url)
    paramsDF.to_sql(table_name, engine, if_exists='append')

def paramProcessor(conn, account_id, query_end_date, lower_bound, num_days, start_hour, end_hour, hour_bucket, hour_array, version):
    conn = sql_connect()
    historyDF = sql_query(conn, account_id, query_end_date, num_days, start_hour, end_hour, hour_bucket)
    historyDF_clean = clean_data(historyDF, hour_array, hour_bucket)
    historyDF_format = format_data(historyDF_clean, hour_array)
    paramsDF = calc_params(historyDF_format, account_id, lower_bound, query_end_date, num_days, hour_array, version)
    return paramsDF

###############################################################################################
# settings 
###############################################################################################
write_url = 'postgresql://localhost:5432/jyfan'
write_table_name = 'anomaly_detection_v0_1'
lower_bound = 10
num_days = 30
start_hour = 0
end_hour = 6
hour_bucket = 2
hour_array = np.asarray([0,2,4])
version='iqr'

if __name__=="__main__":
    query_end_date = sys.argv[1]

    conn = sql_connect()
    for account_id in [1]:
        paramsDF = paramProcessor(conn, account_id, query_end_date, lower_bound, num_days, start_hour, end_hour, hour_bucket, hour_array, version)
        write_params(paramsDF, write_url, write_table_name) 

