import logging
import logging.config
import sys
import yaml
from pytz import timezone
from datetime import datetime
from datetime import timedelta
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
        tracker = app.Tracker(config['redis'])

        conn_sensors = psycopg2.connect(**config['sensors_db'])
        conn_anomaly = psycopg2.connect(**config['anomaly'])

        dbscan_params_meta = config['dbscan_params_meta']
        
        account_ids = app.get_active_accounts(conn_sensors)
        logger.debug("Found %d account_ids", len(account_ids))
        
        for account_id in account_ids:
            logger.debug("Iteration on account_id %d", account_id)
            if tracker.seen_before(account_id):
                logger.debug("Skipping account: %d since we've already seen it", account_id)
                continue
            run_success = app.run(account_id, conn_sensors, conn_anomaly, dbscan_params_meta)        
            if run_success:
                tracker.track(account_id)
                logger.debug("Processed %s", account_id)

        logger.info("Iteration done")
        logger.info("Tracker has keys %s", tracker.query_keys())
        logger.info("For date %s currently %d unique account_ids tracked out of roughly %d accounts attempted", tracker.tracking_key, len(tracker.query_tracking_key()), len(account_ids))

if __name__ == '__main__':
    main()
