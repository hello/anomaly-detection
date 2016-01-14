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

    while True:
        tracker = app.Tracker(config['redis'])

        conn_sensors = psycopg2.connect(**config['sensors_db'])
        conn_anomaly = psycopg2.connect(**config['anomaly'])

        dbscan_params_meta = config['dbscan_params_meta']
        
        account_ids = app.get_active_accounts(conn_sensors)
        logging.debug("Found %d account_ids", len(account_ids))
        
        for account_id in account_ids:
            if tracker.seen_before(account_id):
                logging.debug("Skipping account: %d since we've already seen it", account_id)
                continue
            app.run(account_id, conn_sensors, conn_anomaly, dbscan_params_meta)        
            tracker.track(account_id)
            logging.debug("Processed %s", account_id)
        logging.warn("Iteration done")

if __name__ == '__main__':
    main()
