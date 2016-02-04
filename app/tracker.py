import redis
from datetime import datetime
from pytz import timezone
from hellopy import DATE_FORMAT, DATE_HR_FORMAT

class Tracker(object):
    def __init__(self, redis_config):
        self.redis_config = redis_config
        self.r = redis.Redis(**redis_config)
        now_utc = datetime.utcnow()
        self.success_key = "%s|%s" % ("suripu-anomaly-success", now_utc.strftime(DATE_FORMAT))
        self.r.expire(self.success_key, 3600 * 12)
#        self.r.sadd(self.success_key, -999)

        self.fail_key = "%s|%s" %("suripu-anomaly-fail", now_utc.strftime(DATE_HR_FORMAT))
        self.r.expire(self.fail_key, 3600 * 1)
#        self.r.sadd(self.fail_key, -999)

    def track_success(self, account_id):
        self.r.sadd(self.success_key, account_id)

    def track_fail(self, account_id):
        self.r.sadd(self.fail_key, account_id)

    def seen_before(self, account_id):
        return self.r.sismember(self.success_key, account_id) or self.r.sismember(self.fail_key, account_id)

    def query_success_key(self):
        return self.r.smembers(self.success_key)

    def query_fail_key(self):
        return self.r.smembers(self.fail_key)

    def query_keys(self):
        return self.r.keys()

    def ping(self):
        return self.r.ping()

