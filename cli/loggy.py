

import logging
from timer import Timer


def setup_logger():

    log_format = "%(levelname)s | %(asctime)s | %(name)s | %(message)s"

    logging.basicConfig(filename="FEAT.log",
                        filemode="w",
                        format=log_format,
                        level=logging.INFO)
    return logging.getLogger()


def log_machine(f):

    def wrapper(*args, **kwargs):

        # start the timer, initiate message to log file
        t = Timer()
        t.start()

        logger = logging.getLogger(__name__)
        log_message = ('{} | start'.format(f.__name__))
        logger.info(log_message)

        # execute the called function
        returned_objs = f(*args, **kwargs)

        # stop the timer, log exit
        log_message = ('%s | complete | %.2f sec' % (f.__name__, t.stop()))
        logger.info(log_message)

        return returned_objs

    return wrapper
