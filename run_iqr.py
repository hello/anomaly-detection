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

import datetime
from datetime import date, timedelta

from sklearn.cluster import DBSCAN
from sklearn import metrics
from sklearn import svm

from iqr import *

###############################################################################################
# settings 
###############################################################################################

anomaly_write_url = 'postgresql://localhost:5432/jyfan'
anomaly_write_table_name = 'anomaly'

if __name__ == "__main__":
    accountDict = loadDictionary()
    anomaly_date = "2015-11-17"

    conn_last_night = sql_connect_last_night()
    conn_params = sql_connect_params()
    conn_anomaly = sql_connect_anomaly()

    for account_id in [21561]:
        last_nightDF = sql_query_last_night(conn_last_night, account_id, anomaly_date)
        if not check_completeDF(last_nightDF):
            continue  
        paramsDF = sql_query_params(conn_params)

        resultsDF = iqr_helper(last_nightDF, paramsDF, account_id, anomaly_date)
        write_anomaly(resultsDF, anomaly_write_url, anomaly_write_table_name)



