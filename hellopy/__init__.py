import logging
import logging.config

import time

logging.config.fileConfig('logging.ini')
logger = logging.getLogger(__name__)

def timeit(fn):
    def timed(*args, **kw):
        ts = time.time()
        result = fn(*args, **kw)
        te = time.time()
        
        logging.debug('%r (%r, %r) %2.2f sec', fn.__name__, args, kw, te - ts)
        return result

    return timed