import logging


def setup_logger(peer_mid, name, log_file, level=None, mode='w'):
    if not level:
        level = logging.ERROR
    formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s')

    handler = logging.StreamHandler() #logging.FileHandler(log_file, mode=mode)
    handler.setFormatter(formatter)

    logger = logging.getLogger(name + "_" + peer_mid)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger
