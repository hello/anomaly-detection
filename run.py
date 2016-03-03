import logging
import logging.config
import sys
import yaml
from pytz import timezone
from datetime import datetime
from datetime import timedelta
import time
import psycopg2


logging.config.fileConfig('logging.ini')
logger = logging.getLogger(__name__)


import app

def main():

    logger.info("test")
    config = {}
    with open(sys.argv[1], 'r') as f:
        c = yaml.load(f)
        config.update(c)
    logger.info("Loaded configurations %s", config)

    while True:
        iteration_start = datetime.now()
        try:
            tracker = app.Tracker(config['redis'])
            tracker.ping()
            logger.info("Pinged redis")
        except:
            logger.debug("Unable to ping redis; sleeping for 10 min")
            time.sleep(60 * 10)
            continue

        try:
            conn_sensors = psycopg2.connect(**config['sensors_db'])
            conn_anomaly = psycopg2.connect(**config['anomaly'])
            logger.info("Connected to sensors_db and anomaly")
        except psycopg2.Error as error:
            logger.debug(error)
            logger.debug("Sleeping for 10 min")
            time.sleep(60 * 10)
            continue

        questions_endpt_params = config['questions_endpt_params']
        dbscan_params_meta = config['dbscan_params_meta']

        account_ids = app.get_active_accounts(conn_sensors)
        logger.debug("Found %d account_ids", len(account_ids))

        no_accounts_processed = True        
        for account_id in account_ids:
#            logger.debug("Iteration on account_id %d", account_id)
            if tracker.seen_before(account_id):
#                logger.debug("Skipping account: %d since we've already seen it", account_id)
                continue
            no_accounts_processed = False
            run_success = app.run(account_id, conn_sensors, conn_anomaly, dbscan_params_meta, questions_endpt_params)        
            if run_success:
                tracker.track_success(account_id)
            else:
                tracker.track_fail(account_id)
#            logger.debug("Processed %s", account_id)

        iteration_end = datetime.now()
        iteration_mins = (iteration_end - iteration_start)/60.0
        iteration_start_str = datetime.strftime(iteration_start, "%Y-%m-%d %H:%M:%S:%f")
        iteration_end_str = datetime.strftime(iteration_end, "%Y-%m-%d %H:%M:%S:%f")
        logger.info("Iteration done took %d mins start: %s end: %s", iterations_mins, iteration_start_str, iteration_end_str)
#        logger.info("Tracker has keys %s", tracker.query_keys())
        logger.info("For date %s currently %d success unique account_ids %d fail unique account_ids tracked out of roughly %d accounts attempted", tracker.success_key, len(tracker.query_success_key()), len(tracker.query_fail_key()), len(account_ids))

        if no_accounts_processed:
            logger.info("No accounts processed on last loop because all tracked. Sleeping for 5 min")
            time.sleep(60 * 5) 

if __name__ == '__main__':
    main()
