import logging
from configparser import ConfigParser

def get_logger(log_file):
    logger = logging.getLogger("ETL_LOGGER")
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler(log_file)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    return logger