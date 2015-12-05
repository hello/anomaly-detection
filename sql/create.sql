CREATE TABLE anomaly_results(
  id BIGSERIAL PRIMARY KEY,
  account_id BIGINT,
  date_computed TIMESTAMP,
  target_date TIMESTAMP,
  anomaly_days TIMESTAMP[],
  alg_id INTEGER
);

/* anomaly_days array of timestamps of days identified as anomalous by alg run for target_date 
 dates sorted from most recent => furthest away 
 anomaly_days = "{ TIMESTAMP "2015-11-01", TIMESTAMP "2015-10-31" }"*/

CREATE TABLE anomaly_alg(
  alg_id INTEGER PRIMARY KEY,
  alg_category VARCHAR,
  alg_params TEXT,
  num_buckets INTEGER,
  target_window INTEGER
);

/* alg_params = "{alg: DBSCAN, feature: ambient_light, num_buckets: 3, a: 1.5, b: 2, minPts:4}" */
