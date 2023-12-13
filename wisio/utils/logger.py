import logging
from time import perf_counter


class ElapsedTimeLogger(object):

    def __init__(self, message: str, level=logging.INFO, stacklevel=3):
        self.level = level
        self.message = message
        self.stacklevel = stacklevel

    def __enter__(self):
        self.start_time = perf_counter()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end_time = perf_counter()
        self.elapsed_time = self.end_time - self.start_time
        if self.level is logging.DEBUG:
            logging.debug(
                msg=f"{self.message} ({self.elapsed_time})",
                stacklevel=self.stacklevel,
            )
        else:
            logging.info(
                msg=f"{self.message} ({self.elapsed_time})",
                stacklevel=self.stacklevel,
            )


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
