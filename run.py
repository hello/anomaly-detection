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
    logger.info("configs_loaded=%s", config)

    isodd = int(sys.argv[2])%2

    while True:
        iteration_start = datetime.now()
        try:
            tracker = app.Tracker(config['redis'])
            tracker.ping()
            logger.info("action=pinged_redis")
        except:
            logger.debug("error=unable_to_ping_redis sleep=10min")
            time.sleep(60 * 10)
            continue

        try:
            conn_sensors = psycopg2.connect(**config['sensors_db'])
            conn_anomaly = psycopg2.connect(**config['anomaly'])
            logger.info("conn_psycopg=True")
        except psycopg2.Error as error:
            logger.debug(error)
            logger.debug("error=psycopg_conn sleep=10min")
            time.sleep(60 * 10)
            continue

        questions_endpt_params = config['questions_endpt_params']
        dbscan_params_meta = config['dbscan_params_meta']

        account_ids = app.get_active_accounts(conn_sensors, isodd)
        logger.debug("eligible_accounts=%d", len(account_ids))

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
        iteration_mins = (iteration_end - iteration_start).total_seconds()/60.0
        iteration_start_str = datetime.strftime(iteration_start, "%Y-%m-%d %H:%M:%S:%f")
        iteration_end_str = datetime.strftime(iteration_end, "%Y-%m-%d %H:%M:%S:%f")
        logger.info(""" action=Iteration_done 
                        iter_duration_mins=%d 
                        iter_start=%s 
                        iter_end=%s 
                        iter_date=%s
                        insert_success=%d
                        insert_fail=%d
                        insert_attempted=%d """, iteration_mins, iteration_start_str, iteration_end_str, tracker.success_key, len(tracker.query_success_key()), len(tracker.query_fail_key()), len(account_ids))
#        logger.info("Tracker has keys %s", tracker.query_keys())

        if no_accounts_processed:
            logger.info("reason=no_accounts_processed_last_loop sleep=5min")
            time.sleep(60 * 5) 

if __name__ == '__main__':
    main()
