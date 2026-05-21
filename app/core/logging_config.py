from sys import stdout

from loguru import logger


def setup_logging():
    logger.remove()
    logger.add(
        stdout,
        level="INFO",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | APP | {message} | {extra}",
        backtrace=False,
        diagnose=False
    )