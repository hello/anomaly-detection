import psycopg2
import datetime
from datetime import datetime

import logging
logger = logging.getLogger(__name__)

def write_anomaly_result(conn, account_id, target_date, alg_id):
    """
    input:  - connection conn
            - int or string account_id
            - datetime target_date
            - int or string alg_id 

    output: 
            - boolean - successful insert or not 
    """
    account_id = str(account_id)
    alg_id = str(alg_id)

    try:
        with conn.cursor() as cur:
            query = cur.mogrify("INSERT INTO anomaly_results (account_id, target_date, alg_id) VALUES (%s, %s, %s) RETURNING id", 
                                (account_id, target_date, alg_id))
            logging.info("query: %s", query)
            cur.execute(query)
            inserted_row = cur.fetchone()

            conn.commit()

            if inserted_row:
                row_id = int(inserted_row[0])
                logging.info("Success insertion into anomaly_results for account_id=%s target_date=%s alg_id=%s row_id=%s.", account_id, target_date, alg_id, row_id)
                return True

    except psycopg2.Error as error:
        logging.error("Fail insertion into anomaly_results for account_id=%s target_date=%s alg_id=%s psycopg2 error=%s." 
                    %(account_id, target_date, alg_id, error))
        conn.rollback()

    return False

def write_anomaly_result_raw(conn, account_id, target_date, anomaly_days, alg_id):
    """
    input:  - connection conn
            - int or string account_id
            - datetime target_date
            - datetime[] anomaly_days
            - int or string alg_id 

    output: 
            - boolean - successful insert or not 

    logger:
            http://www.postgresql.org/docs/current/static/errcodes-appendix.html#ERRCODES-TABLE
    """
    account_id = str(account_id)
    alg_id = str(alg_id)

    try:
        with conn.cursor() as cur:
            query = cur.mogrify("INSERT INTO anomaly_results_raw (account_id, target_date, anomaly_days, alg_id) VALUES (%s, %s, %s, %s) RETURNING id", 
                                (account_id, target_date, anomaly_days, alg_id))
            logging.info("query: %s", query)
            cur.execute(query)
            inserted_row = cur.fetchone()

            conn.commit()

            if inserted_row:
                row_id = int(inserted_row[0])
                logging.info("Success insertion into anomaly_results_raw for account_id=%s target_date=%s alg_id=%s row_id=%s.", account_id, target_date, alg_id, row_id)
                return True

    except psycopg2.Error as error:
        logging.error("Fail insertion into anomaly_results_raw for account_id=%s target_date=%s alg_id=%s psycopg2 error=%s." 
                    %(account_id, target_date, alg_id, error))
        conn.rollback()
    return False

def get_num_anomalies_date(conn, date_computed, target_date, alg_id):
    alg_id = str(alg_id)

    try:
        with conn.cursor() as cur:
            query = cur.mogrify("SELECT COUNT(*) FROM anomaly_results_raw WHERE DATE_TRUNC('day', date_computed)=%s " + 
                                                                    "AND target_date=%s " +
                                                                    "AND alg_id=%s " +
                                                                    "AND target_date = anomaly_days[1]  ", 
                                (date_computed, target_date, alg_id))
            logging.info("query: %s", query)
            cur.execute(query)
            count = cur.fetchall()[0][0]
            return count

    except psycopg2.Error as error:
        logging.error("Fail to query anomaly_results_raw for date_computed=%s target_date=%s alg_id=%s", date_computed, target_date, alg_id)
        conn.rollback()

    return -1

def get_num_alg_date(conn, date_computed, target_date, alg_id):
    alg_id = str(alg_id)

    try:
        with conn.cursor() as cur:
            query = cur.mogrify("SELECT COUNT(*) FROM anomaly_results_raw WHERE DATE_TRUNC('day', date_computed)=%s " + 
                                                                    "AND target_date=%s " +
                                                                    "AND alg_id=%s ",
                                (date_computed, target_date, alg_id))
            logging.info("query: %s", query)
            cur.execute(query)
            count = cur.fetchall()[0][0]
            return count

    except psycopg2.Error as error:
        logging.error("Fail to query anomaly_results_raw for date_computed=%s target_date=%s alg_id=%s", date_computed, target_date, alg_id)

    return -1

def get_anomaly_density_date(conn, date_computed, target_date, alg_id):
    alg_id = str(alg_id)

    try:
        with conn.cursor() as cur:
            query = cur.mogrify("SELECT ARRAY_LENGTH(anomaly_days, 1) FROM anomaly_results_raw " +
                                                                    "WHERE DATE_TRUNC('day', date_computed)=%s " + 
                                                                    "AND target_date=%s " +
                                                                    "AND alg_id=%s ",
                                (date_computed, target_date, alg_id))
            logging.info("query: %s", query)
            cur.execute(query)
            counts = [item[0] for item in cur.fetchall()]
            return counts

    except psycopg2.Error as error:
        logging.error("Fail to query anomaly_results_raw for date_computed=%s target_date=%s alg_id=%s", date_computed, target_date, alg_id)

    return [-1]

def get_avg_anomaly_density_date(conn, date_computed, target_date, alg_id):
    alg_id = str(alg_id)

    try:
        with conn.cursor() as cur:
            query = cur.mogrify("SELECT AVG(ARRAY_LENGTH(anomaly_days, 1)) FROM anomaly_results_raw " +
                                                                    "WHERE DATE_TRUNC('day', date_computed)=%s " + 
                                                                    "AND target_date=%s " +
                                                                    "AND alg_id=%s ",
                                (date_computed, target_date, alg_id))
            logging.info("query: %s", query)
            cur.execute(query)
            count = cur.fetchall()
            return count[0][0]

    except psycopg2.Error as error:
        logging.error("Fail to query anomaly_results_raw for date_computed=%s target_date=%s alg_id=%s", date_computed, target_date, alg_id)

    return -1.0
