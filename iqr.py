import numpy as np
import pandas as pd

import matplotlib.pyplot as plt
from matplotlib import cm
from mpl_toolkits.mplot3d import Axes3D
import datetime
from datetime import date
import sys
import itertools

import psycopg2
from sqlalchemy import create_engine

import datetime
from datetime import date, timedelta

from sklearn.cluster import DBSCAN
from sklearn import metrics
from sklearn import svm

###############################################################################################
# query last night
###############################################################################################
def sql_connect_last_night():
    try:
        connection = psycopg2.connect("host=sensors2.cy7n0vzxfedi.us-east-1.redshift.amazonaws.com user=jyfan password=HelloRedshiftSensor1 port=5439 dbname=sensors1")
        return connection
    except:
        print "Unable to connect to database to get last night's data"

def sql_query_last_night(conn, account_id, morning_date, start_hour=0, end_hour=6, hour_bucket=2):
    """
       hour  avg_light  count
    0     0         10    120
    1     2          9    120
    2     4         10    120
    """
    cur = conn.cursor()
    
    end_query_date = datetime.datetime.strptime(morning_date, '%Y-%m-%d').date()
    start_query_date = end_query_date - timedelta(days=1) 

    query = """SELECT MIN(EXTRACT(hour FROM local_utc_ts)) AS hour, 
                AVG(ambient_light) AS avg_light, COUNT(ambient_light) AS count FROM device_sensors_master 
                    WHERE account_id='%d' 
                        AND (local_utc_ts > '%s' AND local_utc_ts <= '%s')
                        AND (EXTRACT(hour FROM local_utc_ts) >= %d AND EXTRACT(hour FROM local_utc_ts) < %d)
                    GROUP BY (EXTRACT(hour FROM local_utc_ts) / %d)
                    ORDER BY hour;""" % (account_id, '{:%Y-%m-%d}'.format(start_query_date), '{:%Y-%m-%d}'.format(end_query_date), start_hour, end_hour, hour_bucket)
    try:
        cur.execute(query)
    except psycopg2.Error as error:
        print error.pgcode 

    last_night = cur.fetchall()

    last_night_array = np.asarray(last_night)
    last_nightDF = pd.DataFrame(last_night_array, columns=['hour','avg_light','count'])
#    print last_nightDF
    return last_nightDF 

def check_completeDF(last_nightDF, hour_bucket=2, hour_array=np.asarray([0,2,4])):
    for hour in hour_array:
        if last_nightDF[last_nightDF['hour']==hour]['count'].item() < (hour_bucket * 60):
            return False
    return True

###############################################################################################
# query params 
###############################################################################################
def sql_connect_params():
    try:
        connection = psycopg2.connect("host=localhost user=jyfan port=5432 dbname=jyfan")
        return connection
    except:
        print "Unable to connect to database to get last night's data"

def sql_query_params(conn, param_date ='recent', param_table_name='anomaly_detection_v0_1', hour_array=np.asarray([0,2,4])):
    """
      account_id date_computed query_end_date lower_bound num_days version med-0  \
    0          1    2015-11-18     2015-11-01          10       30     iqr  11.5   

      left-0   right-0 med-2 left-2 right-2 med-4 left-4 right-4  
    0  10.45  15021.95    11     10      12    11   9.45      12  
    """

    titleArray = ['param_id','account_id','date_computed','query_end_date','lower_bound','num_days','version']

    cur = conn.cursor()
    for hour in hour_array:
        medTitle = 'med-' + str(hour)
        leftTitle = 'left-' + str(hour)
        rightTitle = 'right-' + str(hour)       
        titleArray.extend((medTitle, leftTitle, rightTitle))

    if param_date == 'recent':
        query = "SELECT * FROM %s ORDER BY date_computed LIMIT 1" % param_table_name
    else:
        query = "SELECT * FROM %s WHERE date_computed=%s LIMIT 1" % (param_table_name, param_date)

    try:
        cur.execute(query)
    except psycopg2.Error as error:
        print error.pgcode
    
    params = cur.fetchall()
    paramsArray = np.asarray(params)[0][:][np.newaxis] 
    paramsDF = pd.DataFrame(paramsArray, columns=titleArray)
    return paramsDF 

###############################################################################################
# iqr 
###############################################################################################
def iqr_helper(last_nightDF, paramsDF, account_id, anom_date, hour_array = np.asarray([0,2,4]), extremity=6):

    anom_list = []
    anom_result = 0
    for hour in hour_array:
        data = last_nightDF[last_nightDF['hour']==hour]['avg_light'].item()
        med = paramsDF['med-' + str(hour)].item()
        left = paramsDF['left-' + str(hour)].item()
        right = paramsDF['right-' + str(hour)].item()
        print data, med, left, right

        result = iqr(data, med, left, right, extremity)
        anom_result += result

        if result == 1:
            anom_list.append(hour)

    titles = ['account_id', 'anom_result',  'anom_list', 'anom_date', 'param_id', 'extremity','version']

    anom_result = not (anom_result == -1 * len(hour_array))   
    param_id = paramsDF['param_id'].item()
    version = 'iqr'

    result_list = np.asarray([account_id, anom_result, anom_list, anom_date, param_id, extremity, version])[np.newaxis]

    resultDF = pd.DataFrame(result_list, columns=titles) 
    return resultDF 

def iqr(data, med, left, right, extremity):
    left_interval = med - (med - left) * extremity
    right_interval = med + (right - med) * extremity
    if left_interval <= data <= right_interval:
        return -1 #not anomaly
    return 1

###############################################################################################
# write anomaly 
###############################################################################################
def sql_connect_anomaly():
    try:
        connection = psycopg2.connect("host=localhost user=jyfan port=5432 dbname=jyfan")
        return connection
    except:
        print "Unable to connect to write iqr result"

def write_anomaly(resultsDF, anomaly_db_url, anomaly_table_name):
    engine = create_engine(anomaly_db_url)
    resultsDF.to_sql(anomaly_table_name, engine, if_exists='append')
