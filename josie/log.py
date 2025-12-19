import logging
import sys
from pathlib import Path
from typing import Optional


def init_logger(logfile: Optional[Path] = None, stdout: bool = False):
    logger = logging.getLogger("JOSIE")
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )

    if logfile and not any(
        isinstance(handler, logging.FileHandler) for handler in logger.handlers
    ):
        file_handler = logging.FileHandler(logfile)
        file_handler.setLevel(logging.DEBUG)  # Set minimum level for file
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    if stdout and not any(
        isinstance(handler, logging.StreamHandler) for handler in logger.handlers
    ):
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)  # Set minimum level for console
        console_handler.setFormatter(formatter)

        logger.addHandler(console_handler)

    return logger
