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
        conn_write = psycopg2.connect(**config['anomaly_results'])
        conn_write_raw = psycopg2.connect(**config['anomaly_results_raw'])

        dbscan_params = config['dbscan_params']
        eps_multi = dbscan_params['eps_multi']
        min_eps = dbscan_params['min_eps']
        min_pts = dbscan_params['min_pts']
        limit = dbscan_params['limit']
        limit_filter = dbscan_params['limit_filter']
        alg_id = dbscan_params['alg_id']
        
        account_ids = app.get_active_accounts(conn_sensors)

        for account_id in account_ids:
            if tracker.seen_before(account_id):
                logging.debug("Skipping account: %d since we've already seen it", account_id)
                continue
            
            app.run(account_id, conn_sensors, conn_write, conn_write_raw, eps_multi, min_eps, min_pts, limit, limit_filter, alg_id)        
            tracker.track(account_id)

        logging.warn("Iteration done")

if __name__ == '__main__':
    main()
