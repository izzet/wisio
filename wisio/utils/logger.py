import logging
from time import perf_counter


class ElapsedTimeLogger(object):
    def __init__(self, message: str, level=logging.INFO):
        self.level = level
        self.message = message

    def __enter__(self):
        self.timer_start = perf_counter()

    def __exit__(self, exc_type, exc_val, exc_tb):
        timer_end = perf_counter()
        elapsed_time = timer_end - self.timer_start
        if self.level is logging.DEBUG:
            logging.debug(f"{self.message} ({elapsed_time})", stacklevel=3)
        else:
            logging.info(f"{self.message} ({elapsed_time})", stacklevel=3)


def setup_logging(filename: str, debug: bool):
    logging.basicConfig(
        datefmt='%H:%M:%S',
        format='[%(levelname)s] [%(asctime)s] %(message)s [%(pathname)s:%(lineno)d]',
        handlers=[
            logging.FileHandler(filename=filename),
            logging.StreamHandler(),
        ],
        level=logging.DEBUG if debug else logging.INFO,
    )
