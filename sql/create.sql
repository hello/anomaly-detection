CREATE TABLE anomaly_results(
  id BIGSERIAL PRIMARY KEY,
  account_id BIGINT,
  date_computed TIMESTAMP,
  target_date TIMESTAMP,
  anomaly_days TIMESTAMP[],
  alg_id INTEGER
);

CREATE TABLE anomaly_alg(
  alg_id INTEGER,
  alg_category VARCHAR,
  alg_params TEXT,
  num_buckets INTEGER,
  target_window INTEGER
);
