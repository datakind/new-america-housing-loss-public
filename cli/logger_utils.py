
"""
utility functions to establish logging functionality
"""

import logging
from timer import Timer


def setup_logger(sys_args: list, config: dict) -> logging.getLoggerClass():
    """
    initiate logger for main routine
    all hard coded options at the moment
    :return: logger object
    """

    log_format = "%(levelname)s | %(asctime)s | %(name)s | %(message)s"

    log_filename = sys_args[0].strip('.py') + '.log'

    logging.basicConfig(filename=log_filename,
                        filemode="w",
                        format=log_format,
                        level=logging.INFO)

    logger = logging.getLogger()

    try:
        if 'logging_level' in config:
            log_level = logging.getLevelName(config['logging_level'])
            logger.setLevel(log_level)
    except ValueError as ve:
        logger.critical('ValueError in setting log level from config file : ' + str(ve))

    return logger


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
        log_message = ('%s | start |' % f.__name__)
        logger.info(log_message)

        # execute the called function
        returned_objs = f(*args, **kwargs)

        # stop the timer, log exit
        log_message = ('%s | complete | %.2f' % (f.__name__, t.stop()))
        logger.info(log_message)

        # return control to calling function
        return returned_objs

    return wrapper

