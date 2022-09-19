import logging
from logging import Logger
from time import perf_counter


class ElapsedTimeLogger(object):
    def __init__(self, logger: Logger, message: str, caller: str = "main"):
        self.caller = caller
        self.logger = logger
        self.message = message

    def __enter__(self):
        self.timer_start = perf_counter()
        return self.logger

    def __exit__(self, exc_type, exc_val, exc_tb):
        timer_end = perf_counter()
        elapsed_time = timer_end - self.timer_start
        self.logger.debug(format_log(caller=self.caller, message=f"{self.message}|{elapsed_time}"))


def create_logger(name: str, log_file: str):
    file_handler = logging.FileHandler(filename=log_file)
    formatter = logging.Formatter("%(asctime)s|%(thread)d|%(funcName)s|%(message)s")
    file_handler.setFormatter(formatter)
    logger = logging.getLogger(name=name)
    logger.addHandler(file_handler)
    logger.setLevel(logging.DEBUG)
    return logger


def format_log(caller: str, message: str):
    return f"{caller}|{message}"
