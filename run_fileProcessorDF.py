import csv
import numpy as np
import pandas as pd

import psycopg2
from sqlalchemy import create_engine
import sys

import datetime
from datetime import date, timedelta

from fileProcessorDF import *

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

