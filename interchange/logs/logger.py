import logging
import logging.handlers
import os
from collections import OrderedDict

import dotenv


class Logger:
    """
    Provides a standardized logger object to print and store log messages.
    """

    _LOG_LEVELS = OrderedDict(
        {
            "debug": logging.DEBUG,
            "info": logging.INFO,
            "warning": logging.WARNING,
            "error": logging.ERROR,
            "critical": logging.CRITICAL,
        }
    )

    _DEFAULT_FMT = "%(asctime)s :: PID %(process)d :: TID %(thread)d :: %(module)s.%(funcName)s :: Line %(lineno)d :: %(levelname)s :: %(message)s"

    def __init__(self, name: str) -> None:
        dotenv.load_dotenv()

        self.logger = logging.getLogger(name)
        self.logger.setLevel(self._LOG_LEVELS[os.environ["ITX_LOG_LEVEL"]])

        format = logging.Formatter(self._DEFAULT_FMT)

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(format)

        file_handler = logging.handlers.TimedRotatingFileHandler(
            filename=os.environ["ITX_LOG_PATH"],
            when="D",
            backupCount=3,
            encoding="utf-8",
        )
        file_handler.setFormatter(format)

        self.logger.addHandler(console_handler)
        self.logger.addHandler(file_handler)
