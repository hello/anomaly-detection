import psycopg2
import os
import numpy as np
import itertools
from pytz import timezone
from datetime import datetime
from datetime import timedelta
# temp

import logging

from sklearn.cluster import DBSCAN


from transformations import from_db_rows
from anomalyDAO import write_anomaly_result, write_anomaly_result_raw

logger = logging.getLogger(__name__)



def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in xrange(0, len(l), n):
        yield l[i:i+n]

DATE_FORMAT = '%Y-%m-%d'


def get_active_accounts(conn):
    now = datetime.now()
    now_utc = now.replace(tzinfo=timezone('UTC'))
    last_night = now_utc + timedelta(days=-2)
    account_ids = set()
    with conn.cursor() as cursor:
        cursor.execute("""SELECT DISTINCT(account_id) FROM tracker_motion_master WHERE local_utc_ts > %(start)s;""", dict(start=last_night))

        rows = cursor.fetchall()
        for row in rows:
            account_ids.add(row[0])
    return account_ids

def normalize_data(compressedMatrix):
    normalizedMatrix = []
    compressedMatrix = np.asarray(compressedMatrix)
    if len(compressedMatrix) == 0:
        return np.asarray([])
    if 0 in compressedMatrix.std(axis=0):
        return np.asarray([])

    normalizedMatrix = (compressedMatrix - compressedMatrix.mean(axis=0))/ compressedMatrix.std(axis=0)
    return np.asarray(normalizedMatrix)

def get_eps(normalizedMatrix):
    if len(normalizedMatrix) <= 1:
        return -1.0 

    distanceList = []
    for pointPair in itertools.combinations(normalizedMatrix, 2):
        distance = np.linalg.norm(pointPair[0] - pointPair[1])
        distanceList.append(distance)
    percDistance = np.percentile(distanceList, 50)
    return percDistance

def feature_extraction(data_dict):
    matrix = []
    sorted_keys = sorted(data_dict.keys())
    

    for day in sorted_keys:
        feature_vector = [np.average(chunk) for chunk in chunks(data_dict[day], 2)]
        # feature_vector = features
        matrix.append(feature_vector)
    return matrix

def run(account_id, conn, conn_write, conn_write_raw, dbscan_params):
    eps_multi = dbscan_params['eps_multi']
    min_eps = dbscan_params['min_eps']
    min_pts = dbscan_params['min_pts']
    limit = dbscan_params['limit']
    limit_filter = dbscan_params['limit_filter']
    alg_id = dbscan_params['alg_id']

    results = []

    now = datetime.now()
    now_utc = now
#    now_utc = now.replace(tzinfo=timezone('UTC'))

    now_start_of_day = now.replace(hour=0).replace(minute=0).replace(second=0).replace(microsecond=0)

    thirty_days_ago = now_utc + timedelta(days=-limit)
    with conn.cursor() as cursor:
        cursor.execute("""SELECT SUM(ambient_light), count(1), date_trunc('hour', local_utc_ts) AS hour
                          FROM device_sensors_master
                          WHERE account_id = %(account_id)s
                          AND local_utc_ts > %(start)s
                          AND local_utc_ts < %(end)s
                          AND extract('hour' from local_utc_ts) < 6
                          GROUP BY hour
                          ORDER BY hour ASC""", dict(account_id=account_id, start=thirty_days_ago, end=now_utc))

        rows = cursor.fetchall()
        for row in rows:
            results.append(row)
    
    if not results:
        logging.warn("No data for user %d", account_id)
        return

    days = from_db_rows(results)

    if len(days) < limit_filter:
        logging.warn("not enough days (%d) for user %d", len(days), account_id)
        return
    
    sorted_days = sorted(days.keys())
    matrix = feature_extraction(days)
    if not matrix:
        logging.error("No feature extracted. Error?")
        return
    
    normalized_features = normalize_data(matrix)
    if len(normalized_features) <= 1:
        logging.warn("Normalized features empty. len(normalized_features)=%s" % str(len(normalized_features)) )
        return

    eps = get_eps(normalized_features)
    if eps <= 0:
        logging.error("Incorrect input passed to get_eps() eps=%s." % eps)
        return

    db = DBSCAN(eps=max(eps_multi*eps, min_eps), min_samples=min_pts)
    db.fit(normalized_features)
    labels = db.labels_

    anomaly_days = []
    for day, anomaly in zip(sorted_days, labels):
        if anomaly == -1:
            anomaly_days.append(datetime.strptime(day, DATE_FORMAT))
            logging.info("%s is an anomaly for account %d", day, account_id)

    if now_start_of_day in anomaly_days:
        write_anomaly_result(conn_write, account_id, now_start_of_day, alg_id)

    anomaly_days.reverse() #store most recent anomaly first for easy query 
    write_anomaly_result_raw(conn_write_raw, account_id, now_start_of_day, anomaly_days, alg_id)

if __name__ == '__main__':
    main()
