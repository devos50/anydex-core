import logging
from asyncio import get_event_loop


class LoopFilter(logging.Filter):
    def filter(self, record):
        loop = get_event_loop()
        record.loop_time = "%.1f" % loop.time()
        return True


def setup_logger(peer_mid, name, log_file, level=None, mode='w'):
    if not level:
        level = logging.ERROR
    formatter = logging.Formatter('%(loop_time)s %(name)s %(levelname)s %(message)s')

    handler = logging.StreamHandler() #logging.FileHandler(log_file, mode=mode)
    handler.setFormatter(formatter)

    logger = logging.getLogger(name + "_" + peer_mid)
    logger.setLevel(level)
    logger.addHandler(handler)
    logger.addFilter(LoopFilter())

    return logger
