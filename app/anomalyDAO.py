import psycopg2

import logging
logger = logging.getLogger(__name__)

def write_anomaly_result(conn, account_id, date_computed, target_date, anomaly_days, alg_id):
    """
    input:  - connection conn
            - int or string account_id
            - datetime date_computed 
            - datetime target_date
            - datetime[] anomaly_days
            - int or string alg_id 

    output: 
            - tuple (row_id, psql error code) 
            http://www.postgresql.org/docs/current/static/errcodes-appendix.html#ERRCODES-TABLE
            row_id=-1 if error present
    """
    account_id = str(account_id)
    alg_id = str(alg_id)

    try:
        with conn.cursor() as cur:
            query = cur.mogrify("INSERT INTO anomaly_results (account_id, date_computed, target_date, anomaly_days, alg_id) VALUES (%s, %s, %s, %s, %s) RETURNING id", 
                                (account_id, date_computed, target_date, anomaly_days, alg_id))
            logging.info("query: %s", query)
            cur.execute(query)
            inserted_row = cur.fetchone()

            conn.commit()

            if inserted_row:
                row_id = int(inserted_row[0])
                logging.info("Success insertion into anomaly_results for account_id=%s date_computed=%s target_date=%s alg_id=%s.", account_id, row_id)
                return (row_id, '00000')

    except psycopg2.Error as error:
        logging.error("Fail insertion into anomaly_results for account_id=%s date_computed=%s target_date=%s alg_id=%s psycopg2 error=%s." 
                    %(account_id, date_computed, target_date, alg_id, error))
    return (-1, error.pgcode) 

def get_num_anomalies_date(conn, date_computed, target_date, alg_id):
    alg_id = str(alg_id)

    try:
        with conn.cursor() as cur:
            query = cur.mogrify("SELECT COUNT(*) FROM anomaly_results WHERE DATE_TRUNC('day', date_computed)=%s " + 
                                                                    "AND DATE_TRUNC('day', target_date)=%s " +
                                                                    "AND alg_id=%s " +
                                                                    "AND DATE_TRUNC('day', target_date) = DATE_TRUNC('day', anomaly_days[1])  ", 
                                (date_computed, target_date, alg_id))
            logging.info("query: %s", query)
            cur.execute(query)
            count = cur.fetchall()[0][0]
            return count

    except psycopg2.Error as error:
        logging.error("Fail to query anomaly_results for date_computed=%s target_date=%s alg_id=%s", date_computed, target_date, alg_id)

    return -1

def get_num_alg_date(conn, date_computed, target_date, alg_id):
    alg_id = str(alg_id)

    try:
        with conn.cursor() as cur:
            query = cur.mogrify("SELECT COUNT(*) FROM anomaly_results WHERE DATE_TRUNC('day', date_computed)=%s " + 
                                                                    "AND DATE_TRUNC('day', target_date)=%s " +
                                                                    "AND alg_id=%s ",
                                (date_computed, target_date, alg_id))
            logging.info("query: %s", query)
            cur.execute(query)
            count = cur.fetchall()[0][0]
            return count

    except psycopg2.Error as error:
        logging.error("Fail to query anomaly_results for date_computed=%s target_date=%s alg_id=%s", date_computed, target_date, alg_id)

    return -1

def get_anomaly_density_date(conn, date_computed, target_date, alg_id):
    alg_id = str(alg_id)

    try:
        with conn.cursor() as cur:
            query = cur.mogrify("SELECT ARRAY_LENGTH(anomaly_days, 1) FROM anomaly_results " +
                                                                    "WHERE DATE_TRUNC('day', date_computed)=%s " + 
                                                                    "AND DATE_TRUNC('day', target_date)=%s " +
                                                                    "AND alg_id=%s ",
                                (date_computed, target_date, alg_id))
            logging.info("query: %s", query)
            cur.execute(query)
            counts = [item[0] for item in cur.fetchall()]
            return counts

    except psycopg2.Error as error:
        logging.error("Fail to query anomaly_results for date_computed=%s target_date=%s alg_id=%s", date_computed, target_date, alg_id)

    return [-1]

def get_avg_anomaly_density_date(conn, date_computed, target_date, alg_id):
    alg_id = str(alg_id)

    try:
        with conn.cursor() as cur:
            query = cur.mogrify("SELECT AVG(ARRAY_LENGTH(anomaly_days, 1)) FROM anomaly_results " +
                                                                    "WHERE DATE_TRUNC('day', date_computed)=%s " + 
                                                                    "AND DATE_TRUNC('day', target_date)=%s " +
                                                                    "AND alg_id=%s ",
                                (date_computed, target_date, alg_id))
            logging.info("query: %s", query)
            cur.execute(query)
            count = cur.fetchall()
            return count[0][0]

    except psycopg2.Error as error:
        logging.error("Fail to query anomaly_results for date_computed=%s target_date=%s alg_id=%s", date_computed, target_date, alg_id)

    return -1.0
