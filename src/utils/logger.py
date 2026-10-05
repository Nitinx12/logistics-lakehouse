import logging
import os

# single-line format shared by every module logger
_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"

_configured = False


# returns a module logger, configuring the root handler once from LOG_LEVEL
def get_logger(name: str) -> logging.Logger:
    global _configured
    if not _configured:
        level = getattr(
            logging, (os.getenv("LOG_LEVEL") or "INFO").upper(), logging.INFO
        )
        logging.basicConfig(level=level, format=_FORMAT)
        _configured = True
    return logging.getLogger(name)
