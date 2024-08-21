from _pydwarfs import *

import logging
import pydwarfs.reader

__version__ = "0.10.1"


class python_logger(logger):
    level_map = {
        logger.TRACE: logging.DEBUG,
        logger.DEBUG: logging.DEBUG,
        logger.VERBOSE: logging.INFO,
        logger.INFO: logging.INFO,
        logger.WARN: logging.WARNING,
        logger.ERROR: logging.ERROR,
        logger.FATAL: logging.CRITICAL,
    }

    def __init__(self, level=logger.INFO):
        self._logger = logging.getLogger("pydwarfs")
        super().__init__(level)

    def write(self, level, msg, file, line):
        rec = self._logger.makeRecord(
            "pydwarfs", self.level_map[level], file, line, msg, None, None
        )
        self._logger.handle(rec)
