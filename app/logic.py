import psycopg2
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

with open('/etc/anomaly_admin_token.txt') as f:
    token = f.read().strip()

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in xrange(0, len(l), n):
        yield l[i:i+n]

DATE_FORMAT = '%Y-%m-%d'

def get_active_accounts(conn, isodd):
    hour_in_millis = 3600000

    now_utc = datetime.utcnow()
    recent_days = now_utc + timedelta(days=-3)
    
    current_utc_hour = now_utc.hour
    allowed_offset_min = int((-current_utc_hour + 6.5) * hour_in_millis)
    allowed_offset_max = (-current_utc_hour + 8) * hour_in_millis

    account_ids = set()
    with conn.cursor() as cursor:
        isodd = str(isodd)
        cursor.execute("""SELECT DISTINCT(account_id), MAX(offset_millis) FROM tracker_motion_master WHERE local_utc_ts > %(start)s AND MOD(account_id,2)=%(odd)s GROUP BY account_id ORDER BY account_id;""", dict(start=recent_days, odd=isodd))

        rows = cursor.fetchall()
        logging.info("active_accounts=%d", len(rows))
        for row in rows:
            if row[1] < allowed_offset_min:
                continue
            elif row[1] > allowed_offset_max:
                continue
            account_ids.add(row[0])
        logging.info("""allowed_offset_min=%d
                        allowed_offset_max=%d
                        eligible_accounts=%d""", allowed_offset_min, allowed_offset_max, len(account_ids))
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
                                FROM (SELECT ambient_light AS ambient_light, local_utc_ts
                                    FROM prod_sense_data
                                    WHERE account_id = %(account_id)s
                                    AND local_utc_ts > %(start)s
                                    AND extract('hour' from local_utc_ts) < 6)
                                GROUP BY hour
                                ORDER BY hour ASC""", dict(account_id=account_id, start=start))
            rows = cursor.fetchall()

            for row in rows:
                results.append(row)

        except psycopg2.Error as e:
            logging.debug("error=psycopg psycopg_error=%s" % e.pgerror) 
            pass

    if len(results)>0:
        logging.info("one_data_length=%d last_local_utc_ts_data=%s" % (len(results), results[-1]))
    else:
        logging.info("one_data_length=%d" % len(results))
    
    return results

def pull_max_lux_data(conn_sensors, date, account_id):
    results = []
    date_string = datetime.strftime(date, DATE_FORMAT)
    single_result = []
    query = "SELECT MAX(ambient_light) FROM device_sensors_master WHERE account_id=%s AND local_utc_ts>='%s 00:00:00' AND local_utc_ts<='%s 04:00:00'" % (account_id, date_string, date_string) 
    with conn_sensors.cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()
    results = rows
    return results

def run_alg(days, dbscan_params, account_id):
    eps_multi = dbscan_params['eps_multi']
    min_eps = dbscan_params['min_eps']
    min_pts = dbscan_params['min_pts']
    limit_filter = int(dbscan_params['limit_filter'])
    alg_id = dbscan_params['alg_id']

    if len(days) < limit_filter:
        logging.warn("not_enough_days=%d limit_filter=%d account_id=%d alg_id=%s", len(days), limit_filter, account_id, alg_id)
        return np.asarray([])

    matrix = feature_extraction(days)
    if not matrix:
        logging.error("error=no_feature_extracted")
        return np.asarray([])
    
    normalized_features = normalize_data(matrix)
    if len(normalized_features) <= 1:
        logging.warn("error=norm_feat_empty len_norm_feat=%s" % str(len(normalized_features)) )
        return np.asarray([])

    eps = get_eps(normalized_features)
    if eps <= 0:
        logging.error("error=incorrect_input_to_get_eps eps=%s." % eps)
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
            logging.info("anom_date=%s account_id=%d", day, account_id)
    return anomaly_days

def write_results(conn_anomaly, account_id, now_start_of_day, dbscan_params, anomaly_days):
    alg_id = dbscan_params['alg_id']
    max_anom_density = dbscan_params['max_anom_density']

    anomaly_days.reverse() #store most recent anomaly first for easy query 
    write_anomaly_result_raw(conn_anomaly, account_id, now_start_of_day, anomaly_days, alg_id)

    if len(anomaly_days) >= max_anom_density:
        logging.info("anom_density=%d acount_id=%d", len(anomaly_days), account_id)
        return False
    if now_start_of_day in anomaly_days:
        return write_anomaly_result(conn_anomaly, account_id, now_start_of_day, alg_id)

def insert_anomaly_question(questions_endpt_params, account_id, sensor, now_date_string):
    url = questions_endpt_params['url']
    headers = {'authorization': token, 'content-type': 'application/json'}
    payload = {'account_id': account_id, 'sensor': sensor, 'night_date': now_date_string}
#    logging.info("PAYLOAD IS %s" %payload)
    try:
        response = requests.post(url, json=payload, headers=headers)
        logging.info("action=request_question_endpt account_id=%d response=%s" % (account_id, response.text))
        return True
    except requests.exceptions.RequestException as e:
        logging.debug(e)
    return False

def run(account_id, conn_sensors, conn_anomaly, dbscan_params_meta, questions_endpt_params):
    """
    returns 'run success' which instructs redis to track account_id or not
    """
    limit = 30

    now = datetime.now()

    #Check that user was home last night
    max_lux_target_date = pull_max_lux_data(conn_sensors, now, account_id)
    if max_lux_target_date[0][0] < 50:
        logging.warn("skip_reason=user_not_home max_lux_target_date=%d account_id=%d", max_lux_target_date[0][0], account_id)
        return True 

    now_date_string = datetime.strftime(now, DATE_FORMAT)
    now_start_of_day = now.replace(hour=0).replace(minute=0).replace(second=0).replace(microsecond=0)
    thirty_days_ago = now + timedelta(days=-limit)

    results = pull_data(conn_sensors, account_id, thirty_days_ago)

    if not results:
        logging.warn("skip_reason=no_data account_id=%d", account_id)
        return True 

    days = from_db_rows(results)

    if len(days) == 0:
        logging.warn("skip_reason=results_pulled_but_no_day_complete_data account_id=%d", account_id)
        return True 

    sorted_days = sorted(days.keys())

    if now_date_string not in days.keys():
        num_days = len(sorted_days)
        earliest_day = sorted_days[0]
        latest_day = sorted_days[-1]
        logging.warn("skip_reason=not_enough_data_target_date target_date=%s account_id=%d num_days_extracted=%d extract_start=%s extrat_end=%s", now, account_id, num_days, earliest_day, latest_day)
        return True 

    for param_index in dbscan_params_meta:
        dbscan_params = dbscan_params_meta[param_index]
        labels = run_alg(days, dbscan_params, account_id)    
        if labels.size == 0:
            logging.info("error=could_not_generate_labels_see_run_alg()")
            continue 
        anomaly_days = get_anomaly_days(sorted_days, labels, account_id)
        wrote_results = write_results(conn_anomaly, account_id, now_start_of_day, dbscan_params, anomaly_days) #Results are not written in anom density too high 
        if not wrote_results:
            continue #Note that if psycopg2 insertion fails, we assume it is because it violated unique index constraint from multiple runs. TODO: specify type of error
        if dbscan_params['insert_question']==False:
            continue #Don't insert question, try the next algorithm
        if now_start_of_day in anomaly_days:
            question_inserted = insert_anomaly_question(questions_endpt_params, account_id, dbscan_params['sensor'], now_date_string)
            if not question_inserted:
                return False 
    return True 

if __name__ == '__main__':
    main()
