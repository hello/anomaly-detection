import logging
import logging.config
import sys
import yaml
import psycopg2
import os
import numpy as np
import itertools
from hellopy import timeit
from pytz import timezone
from datetime import datetime
from datetime import timedelta

# temp

from sklearn.cluster import DBSCAN

logging.config.fileConfig('logging.ini')
logger = logging.getLogger(__name__)

def main():
    logger.info("test")
    config = {}
    with open(sys.argv[1], 'r') as f:
        c = yaml.load(f)
        config.update(c)

    
    conn = psycopg2.connect(**config['sensors_db'])
    
    for account_id in range(1067, 2000):
        do_something(account_id, conn, 6,3)        
    # do_something(1067, conn, 6,3)


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in xrange(0, len(l), n):
        yield l[i:i+n]

DATE_FORMAT = '%Y-%m-%d'


def normalizeData(compressedMatrix):
    normalizedMatrix = []
    compressedMatrix = np.asarray(compressedMatrix)
    if len(compressedMatrix) == 0:
        return np.asarray([])

    normalizedMatrix = (compressedMatrix - compressedMatrix.mean(axis=0))/ compressedMatrix.std(axis=0)
    return np.asarray(normalizedMatrix)

def getEps(normalizedMatrix):
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

def do_something(account_id, conn, a, b):
    results = []

    now = datetime.now()
    now_utc = now.replace(tzinfo=timezone('UTC'))

    limit = 30
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

    days = {}

    """
    { 'today' : [0,0,0,0,0,0]}
    """
    for result in results:

        day = result[2].strftime(DATE_FORMAT)
        hour = result[2].hour
        # if result[1] < 30:
        #     continue
        if day not in days:
            days[day] = [0] * 6

        days[day][hour] = result[0]

    if len(days) < limit / 2:
        logging.warn("not enough days (%d) for user %d", len(days), account_id)
        return
    
    sorted_days = sorted(days.keys())
    matrix = feature_extraction(days)
    normalized_features = normalizeData(matrix)

    eps = getEps(normalized_features)
    db = DBSCAN(eps=max(eps,b), min_samples=5)

    db.fit(normalized_features)
    labels = db.labels_

    for day, anomaly in zip(sorted_days, labels):
        if anomaly == -1:
            logging.info("%s is an anomaly for account %d", day, account_id)

if __name__ == '__main__':
    main()