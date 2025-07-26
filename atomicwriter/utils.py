import logging
import os


def setup_logger(name="atomicwriter", debug_level=False):
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter("[%(levelname)s] %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    # debug_level이 True이면 DEBUG 레벨로 설정, 아니면 INFO 레벨
    if debug_level:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    
    return logger


def check_file_exists(path):
    return os.path.exists(path)
