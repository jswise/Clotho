"""Provide functions for starting the logger."""

import datetime
import logging
import logging.handlers
import pathlib
import sys
import time
import yaml

from clotho.errors import ClothoError


class SubstrateFileHandler(logging.handlers.TimedRotatingFileHandler):
    """Overrides the file handler to prevent multi-line messages."""

    def emit(self, record):
        """Split multi-line messages into multiple messages."""

        for chunk in str(record.msg).split('\n'):
            record.msg = chunk
            super().emit(record)


def check_rollover(handler):
    """Read the first date from the log file, and perform a rollover if it's not today.

    :param SubstrateFileHandler handler: Instantiated handler
    """

    with open(handler.baseFilename, 'r') as log_file:
        first_line = log_file.readline()
    if not first_line:
        return
    log_date = first_line.split(',')[0]
    current_date = datetime.datetime.strftime(datetime.datetime.utcnow(), '%Y-%m-%d')
    if current_date != log_date:
        handler.doRollover()


def get_log_dir(config_path=None):
    """Figure out where to put log files. By default, this is user/TerraconPython/Logs.

    :param config_path: The path to a nonstandard configuration file

    :return: The directory where logs will go
    """

    log_dir = None
    if config_path:
        try:
            with open(config_path, 'r') as stream:
                config_data = yaml.load(stream, Loader=yaml.FullLoader) or {}
                dir_string = config_data.get('log directory')
                if dir_string:
                    log_dir = pathlib.Path(dir_string)
                else:
                    logging.debug('No log directory specified in {}.'.format(config_path))
        except Exception as err:
            print('Failed to load configuration from {0}'.format(config_path))
            print(str(err))

    if not log_dir:
        logging.debug('Using default log location.')

    return log_dir or pathlib.Path.home() / 'TerraconPython' / 'Logs'


def raise_error(message, error_class=ClothoError):
    """Log a message before raising an error.

    :param message: The message to report
    """

    try:
        for chunk in str(message).split('\n'):
            logging.error(chunk)
    except Exception as err:
        print('Cannot write to the log file.')
        print(str(err))

    raise error_class(message)


def start_log_file(log_dir, logger, file_name='PythonLog.csv'):
    """Create the log file and file handler.

    :param log_dir: The directory where logs will go
    :param logger: A Python logger

    :return: The path to the log file
    """

    # Tell the user where the log file will be.
    log_dir.mkdir(parents=True, exist_ok=True)
    logger.info('Log directory:  %s' % log_dir)

    # Create the log file handler.
    log_file_path = log_dir / file_name
    try:
        handler = SubstrateFileHandler(
            log_file_path,
            backupCount=30
        )
    except FileNotFoundError:
        logger.info("Log file location not found. Logging to sys.stdout.")
    except PermissionError:
        logger.info("Log file locked. Logging to sys.stdout.")

    # Set the format for the log text.
    logging.Formatter.converter = time.gmtime
    log_format = '{asctime},{levelname},{message}'
    date_format = '%Y-%m-%d,%H:%M:%S'
    formatter = logging.Formatter(log_format, date_format, '{')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    check_rollover(handler)
    logger.info('Starting {} {}'.format(logger.name, '_' * 80))

    return log_file_path


def start_logging(name=None, **kwargs):
    """Set up logging & log a start message.

    :param name: A name for the logger, typically __name__
    :param log_dir: The directory where logs will go
    :param config_path: The path to a nonstandard configuration file
    :param log_level: The Python logging level
    :param clear_handlers: Indicates whether to remove existing handlers from the logger

    :return: A Python logger
    """

    logger = logging.getLogger(name)
    for handler in logger.handlers:
        if isinstance(handler, SubstrateFileHandler):
            logging.debug('Logger already initialized.')
            return logger
    logger.level = kwargs.get('log_level', logging.INFO)
    if kwargs.get('clear_handlers'):
        logger.handlers.clear()

    # Start with just stdout (print to screen).
    logger.addHandler(logging.StreamHandler(sys.stdout))

    # Get the directory for log files.
    config_path = kwargs.get('config_path')
    log_dir = kwargs.get('log_dir', get_log_dir(config_path))

    start_log_file(log_dir, logger, kwargs.get('file_name', 'PythonLog.csv'))
    return logger
