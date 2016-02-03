import redis
from datetime import datetime
from pytz import timezone
from hellopy import DATE_FORMAT

class Tracker(object):
    def __init__(self, redis_config):
        self.redis_config = redis_config
        self.r = redis.Redis(**redis_config)
        now_utc = datetime.utcnow()
        self.tracking_key = "%s|%s" % ("suripu-anomaly", now_utc.strftime(DATE_FORMAT))
        self.r.expire(self.tracking_key, 3600 * 12)

    def track(self, account_id):
        self.r.sadd(self.tracking_key, account_id)

    def seen_before(self, account_id):
        return self.r.sismember(self.tracking_key, account_id)

    def query_tracking_key(self):
        return self.r.smembers(self.tracking_key)

    def query_keys(self):
        return self.r.keys()
