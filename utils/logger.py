import logging
import os
from pathlib import Path


def getLogger(name: str) -> logging.Logger:
    logLevel = os.getenv("LOG_LEVEL", "INFO")
    logFormat = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    logPath = Path(os.getenv("LOG_PATH", "logs/statly.log"))
    if not logPath.is_absolute():
        logPath = Path(__file__).resolve().parent.parent / logPath
    logPath.parent.mkdir(parents=True, exist_ok=True)

    rootLogger = logging.getLogger()
    if not rootLogger.handlers:
        rootLogger.setLevel(logLevel)

        streamHandler = logging.StreamHandler()
        streamHandler.setFormatter(logging.Formatter(logFormat))
        rootLogger.addHandler(streamHandler)

        fileHandler = logging.FileHandler(logPath, encoding="utf-8")
        fileHandler.setFormatter(logging.Formatter(logFormat))
        rootLogger.addHandler(fileHandler)

    return logging.getLogger(name)
