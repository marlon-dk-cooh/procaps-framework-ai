import logging

LOGGER_ROOT = "procaps-framework-ai"

def configure_logging() -> None:
    logger = logging.getLogger(LOGGER_ROOT)
    logger.handlers.clear()

    level = getattr(logging, "INFO", logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    handler.setFormatter(formatter)
    
    logger.addHandler(handler)
    logger.setLevel(level)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(f"{LOGGER_ROOT}.{name}")