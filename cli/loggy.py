

import logging
from timer import Timer


def setup_logger():
    """
    initiate logger for main routine
    all hard coded options at the moment
    TODO : add filename (log file) and logging level as run-time args or config options
    :return: logger object
    """

    log_format = "%(levelname)s | %(asctime)s | %(name)s | %(message)s"

    logging.basicConfig(filename="FEAT.log",
                        filemode="w",
                        format=log_format,
                        level=logging.INFO)
    return logging.getLogger()


def log_machine(f):
    """
    decorator function to add logging capability
    usage : add @log_machine to functions for which logging is requested
    logs start time and completion time and time to execute each decorated function
    assumes : log file and logger previously initiated by setup_logger()
    :param f: wrapped (decorated) function
    :return: result of wrapper function
    """

    def wrapper(*args, **kwargs):
        """
        executes the logging mechanism at entry and exit of decorated function
        :param args: args supplied to wrapped function
        :param kwargs: kwargs supplied to wrapped function
        :return: returned objs from wrapped function
        """

        # start the timer
        t = Timer()
        t.start()

        # start message to log file
        logger = logging.getLogger(__name__)
        log_message = ('%s | start' % f.__name__)
        logger.info(log_message)

        # execute the called function
        returned_objs = f(*args, **kwargs)

        # stop the timer, log exit
        log_message = ('%s | complete | %.2f sec' % (f.__name__, t.stop()))
        logger.info(log_message)

        # return control to calling function
        return returned_objs

    return wrapper
