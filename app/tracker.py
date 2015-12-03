import redis
from datetime import datetime
from pytz import timezone
from hellopy import DATE_FORMAT

class Tracker(object):
    def __init__(self, redis_config):
        self.redis_config = redis_config
        self.r = redis.Redis(**redis_config)
        now = datetime.now()
        now_utc = now.replace(tzinfo=timezone('UTC'))
        self.tracking_key = "%s|%s" % ("suripu-anomaly", now_utc.strftime(DATE_FORMAT))
        self.r.sadd(self.tracking_key, 0)
        self.r.expire(self.tracking_key, 3600 * 6)

    def track(self, account_id):
        pass

    def seen_before(self, account_id):
        return self.r.sismember(self.tracking_key, account_id)