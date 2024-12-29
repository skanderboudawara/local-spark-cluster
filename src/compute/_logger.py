import logging
from logging.handlers import TimedRotatingFileHandler

# Create and configure a named logger
def setup_logger(name, log_file, level=logging.INFO):
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Create handlers
    file_handler = TimedRotatingFileHandler(filename=log_file, when="D", interval=1, backupCount=2)
    console_handler = logging.StreamHandler()

    # Create formatters and add them to handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # Add handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger

# Set up different named loggers
run_logger = setup_logger('computeLogger', 'compute.log')
db_logger = setup_logger('dbLogger', 'db.log', level=logging.DEBUG)