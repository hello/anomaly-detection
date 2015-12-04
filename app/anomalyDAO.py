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
            cur.execute(query, (str(account_id), str(date_computed), str(target_date), str(anomaly_days), str(alg_id)) )
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

if __name__ == "__main__":
    main()
