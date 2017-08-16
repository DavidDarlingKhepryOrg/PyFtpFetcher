import logging
import sys

from ensure import ensure_annotations


class LoggingUtilities:

    # maximum logging level that
    # will output to the STDOUT stream
    MAX_STDOUT_LEVEL = logging.INFO
    MIN_STDERR_LEVEL = logging.WARNING

    max_stdout_level = MAX_STDOUT_LEVEL
    min_stderr_level = MIN_STDERR_LEVEL

    # instantiate and initialize
    # logging objects and handlers
    dft_msg_format = '%(asctime)s\t%(levelname)s\t%(module)s\t%(funcName)s\t%(lineno)d\t%(message)s'
    dft_date_format = '%Y-%m-%d %H:%M:%S'

    nominal_log_level = logging.INFO

    log_file_mode = 'a'

    new_line = '\n'

    @ensure_annotations
    def get_logger(self,
                   log_name: str='',
                   max_stdout_level: int=logging.INFO,
                   min_stdout_level: int=logging.INFO,
                   min_stderr_level: int=logging.WARNING,
                   dft_msg_format: str=None,
                   dft_date_format: str=None,
                   all_log_file_name: str=None,
                   err_log_file_name: str=None,
                   log_file_mode: str=None):

        err_str = None
        logger = None

        allLoggerFH = None
        errLoggerFH = None

        if max_stdout_level is None:
            max_stdout_level = max_stdout_level
        if min_stderr_level is None:
            min_stderr_level = min_stderr_level
        if dft_msg_format is None:
            dft_msg_format = dft_msg_format
        if dft_date_format is None:
            dft_date_format = dft_date_format
        if log_file_mode is None:
            log_file_mode = log_file_mode

        # set the default logger's values
        logging.basicConfig(level=max_stdout_level,
                            format=dft_msg_format,
                            datefmt=dft_date_format)

        try:
            # instantiate the logger object
            logger = logging.getLogger(log_name)
            # remove log handlers
            logger.handlers = []
        except Exception as err:
            err_str = str(err)
            logging.error('Error instantiating a logger object named: %s', log_name)
            logging.error(err_str)

        if err_str is None:
            try:
                # remove log handlers
                logger.handlers = []
            except Exception as err:
                err_str = str(err)
                logging.error('Error clearing handlers for logger named: %s', log_name)
                logging.error(err_str)

        if err_str is None:
            try:
                # attach stdout to the logger
                # so that outputting to the log also
                # outputs to the stdout console
                logStdOut = logging.StreamHandler(sys.stdout)
                logStdOut.setFormatter(logging.Formatter(dft_msg_format, dft_date_format))
                logStdOut.addFilter(MaxLogLevelFilter(max_stdout_level))
                logStdOut.setLevel(logging.DEBUG)
                logger.addHandler(logStdOut)
            except Exception as err:
                err_str = str(err)
                logging.error('Error instantiating "stdout" handler for logger named: %s', log_name)
                logging.error(err_str)

        if err_str is None:
            try:
                # attach stderr to the logger
                # so that outputting to the log also
                # outputs to the stderr console
                logStdErr = logging.StreamHandler(sys.stderr)
                logStdErr.setFormatter(logging.Formatter(dft_msg_format, dft_date_format))
                logStdErr.addFilter(MinLogLevelFilter(min_stderr_level))
                logStdErr.setLevel(min_stderr_level)
                logger.addHandler(logStdErr)
            except Exception as err:
                err_str = str(err)
                logging.error('Error instantiating "stderr" handler for logger named: %s', log_name)
                logging.error(err_str)

        if err_str is None and all_log_file_name is not None:
            try:
                # instantiate the "all" logging file handler
                allLoggerFH = logging.FileHandler(all_log_file_name)
                allLoggerFH.setLevel(max_stdout_level)
                allLoggerFH.setFormatter(logging.Formatter(dft_msg_format, dft_date_format))
                logger.addHandler(allLoggerFH)
            except Exception as err:
                err_str = str(err)
                logging.error('Error instantiating "all" log handler for logger named: %s', log_name)
                logging.error(err_str)

        if err_str is None and err_log_file_name is not None:
            try:
                # instantiate the "all" logging file handler
                errLoggerFH = logging.FileHandler(err_log_file_name)
                errLoggerFH.setLevel(min_stderr_level)
                errLoggerFH.setFormatter(logging.Formatter(dft_msg_format, dft_date_format))
                logger.addHandler(errLoggerFH)
            except Exception as err:
                err_str = str(err)
                logging.error('Error instantiating "err" log handler for logger named: %s', log_name)
                logging.error(err_str)

        # set the nominal log level
        try:
            logger.setLevel(min_stdout_level)
        except Exception as err:
            err_str = str(err)
            logging.error('Error setting nominal log level for logger named: %s:', log_name)
            logging.error(err_str)

        return logger, allLoggerFH, errLoggerFH, err_str


# ==============================
# implement minimum log filter
# for restraining logging output
# ==============================
class MinLogLevelFilter(logging.Filter):
    '''
    Minimum Log Level Filter class
    '''
    def __init__(self, level):
        '''

        :param level:
        '''
        self.level = level

    def filter(self, record):
        '''

        :param record:
        '''
        return record.levelno >= self.level


# ==============================
# implement maximum log filter
# for restraining logging output
# ==============================
class MaxLogLevelFilter(logging.Filter):
    '''
    Maximum Log Level Filter class
    '''
    def __init__(self, level):
        '''

        :param level:
        '''
        self.level = level

    def filter(self, record):
        '''

        :param record:
        '''
        return record.levelno <= self.level
