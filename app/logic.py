import psycopg2
import os
import numpy as np
import itertools
from pytz import timezone
from datetime import datetime
from datetime import timedelta
import requests
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
    hour_in_millis = 3600000

    now_utc = datetime.utcnow()
    recent_days = now_utc + timedelta(days=-3)
    
    current_utc_hour = now_utc.hour
    allowed_offset = (-current_utc_hour + 6) * hour_in_millis

    account_ids = set()
    with conn.cursor() as cursor:
        cursor.execute("""SELECT DISTINCT(account_id), MAX(offset_millis) FROM tracker_motion_master WHERE local_utc_ts > %(start)s GROUP BY account_id ORDER BY account_id;""", dict(start=recent_days))

        rows = cursor.fetchall()
        logging.info("Select returned %d total active account_ids", len(rows))
        for row in rows:
            if row[1] < allowed_offset:
                continue
            account_ids.add(row[0])
        logging.info("Filtering for current allowed offset_millis>%d returned %d eligible active account_ids", allowed_offset, len(account_ids))
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

def pull_data(conn_sensors, account_id, start):
    results = []
    with conn_sensors.cursor() as cursor:
        try:
            cursor.execute("""SELECT SUM(ambient_light), count(1), date_trunc('hour', local_utc_ts) AS hour
                                FROM (SELECT MAX(ambient_light) AS ambient_light, local_utc_ts
                                    FROM prod_sense_data
                                    WHERE account_id = %(account_id)s
                                    AND local_utc_ts > %(start)s
                                    AND extract('hour' from local_utc_ts) < 6
                                    GROUP BY local_utc_ts)
                                GROUP BY hour
                                ORDER BY hour ASC""", dict(account_id=account_id, start=start))
            rows = cursor.fetchall()

            for row in rows:
                results.append(row)

        except psycopg2.Error as e:
            logging.debug("Encountered psycopg2 error %s" % e.pgerror) 
            pass

    logging.info("pulled data with results length (%d)" % len(results))
    return results

def run_alg(days, dbscan_params, account_id):
    eps_multi = dbscan_params['eps_multi']
    min_eps = dbscan_params['min_eps']
    min_pts = dbscan_params['min_pts']
    limit_filter = dbscan_params['limit_filter']
    alg_id = dbscan_params['alg_id']

    if len(days) < limit_filter:
        logging.warn("not enough days (%d) for user %d", len(days), account_id)
        return np.asarray([])

    matrix = feature_extraction(days)
    if not matrix:
        logging.error("No feature extracted. Error?")
        return np.asarray([])
    
    normalized_features = normalize_data(matrix)
    if len(normalized_features) <= 1:
        logging.warn("Normalized features empty. len(normalized_features)=%s" % str(len(normalized_features)) )
        return np.asarray([])

    eps = get_eps(normalized_features)
    if eps <= 0:
        logging.error("Incorrect input passed to get_eps() eps=%s." % eps)
        return np.asarray([])

    db = DBSCAN(eps=max(eps_multi*eps, min_eps), min_samples=min_pts)
    db.fit(normalized_features)
    labels = db.labels_
    return labels

def get_anomaly_days(sorted_days, labels, account_id):
    anomaly_days = []
    for day, anomaly in zip(sorted_days, labels):
        if anomaly == -1:
            anomaly_days.append(datetime.strptime(day, DATE_FORMAT))
            logging.info("%s is an anomaly for account %d", day, account_id)
    return anomaly_days

def write_results(conn_anomaly, account_id, now_start_of_day, dbscan_params, anomaly_days):
    alg_id = dbscan_params['alg_id']
    max_anom_density = dbscan_params['max_anom_density']

    anomaly_days.reverse() #store most recent anomaly first for easy query 
    write_anomaly_result_raw(conn_anomaly, account_id, now_start_of_day, anomaly_days, alg_id)

    if len(anomaly_days) >= max_anom_density:
        logging.info("Anomaly density %d is too high for account %d", len(anomaly_days), account_id)
        return
    if now_start_of_day in anomaly_days:
        write_anomaly_result(conn_anomaly, account_id, now_start_of_day, alg_id)

def insert_anomaly_question(questions_endpt_params, account_id, sensor, now_date_string):
    url = questions_endpt_params['url']
    headers = {'authorization': questions_endpt_params['authorization'], 'content-type': questions_endpt_params['content-type']}
    payload = "{\n    \"account_id\" : \"%d\",\n    \"sensor\" : \"%s\",\n    \"night_date\": \"%s\" \n}" % (account_id, sensor, now_date_string)
    logging.info("HEADERS ARE %s" % headers)
    logging.info("PAYLOAD IS %s" %payload)

    try:
        response = requests.request("POST", url, data=payload, headers=headers)
        logging.info("Request sent to admin endpoint to insert anomaly question for account_id %d with response %s" % (account_id, response.text))
        return True
    except requests.exceptions.RequestException as e:
        logging.debug(e)
    return False

def run(account_id, conn_sensors, conn_anomaly, dbscan_params_meta, questions_endpt_params):
    limit = 30

    now = datetime.now()
    now_date_string = datetime.strftime(now, DATE_FORMAT)
    now_start_of_day = now.replace(hour=0).replace(minute=0).replace(second=0).replace(microsecond=0)
    thirty_days_ago = now + timedelta(days=-limit)

    results = pull_data(conn_sensors, account_id, thirty_days_ago)

    if not results:
        logging.warn("No data for user %d", account_id)
        return 0 

    days = from_db_rows(results)

    if len(days) == 0:
        logging.warn("Results pulled but no day of complete data for user %d", account_id)
        return 0 

    sorted_days = sorted(days.keys())

    if now_date_string not in days.keys():
        num_days = len(sorted_days)
        earliest_day = sorted_days[0]
        latest_day = sorted_days[-1]
        logging.warn("not enough data on target date (%s) for user %d num_days extracted (%d) from (%s) to (%s)", now, account_id, num_days, earliest_day, latest_day)
        return 0

    for param_index in dbscan_params_meta:
        dbscan_params = dbscan_params_meta[param_index]
        labels = run_alg(days, dbscan_params, account_id)    
        if labels.size == 0:
            logging.info("Could not generate labels, see log message for run_alg()")
            continue
        anomaly_days = get_anomaly_days(sorted_days, labels, account_id)
        write_results(conn_anomaly, account_id, now_start_of_day, dbscan_params, anomaly_days)  
        if dbscan_params['insert_question']=='False':
            continue
        if now_start_of_day in anomaly_days:
            question_inserted = insert_anomaly_question(questions_endpt_params, account_id, dbscan_params['sensor'], now_date_string)
        if not question_inserted:
            return 0
    return 1 

if __name__ == '__main__':
    main()
