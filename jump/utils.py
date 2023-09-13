"""Helper functions"""
from pathlib import Path
import json
import logging

ROOT_PATH = Path(__file__).parent.parent

with open(ROOT_PATH / "config.json", encoding="utf8") as f_in:
    CONFIG = json.load(f_in)

with open(CONFIG["mandatory_columns_path"], encoding="utf8") as f_in:
    FEATURE_SET = f_in.read().splitlines()


def get_logger(name: str, level: str) -> logging.Logger:
    """Create a logger object"""
    logger = logging.getLogger(name)
    if logger.hasHandlers():
        return logger

    logger.setLevel(level)
    sformat = "%(levelname)s:%(asctime)s:%(name)s:%(message)s"
    formatter = logging.Formatter(sformat)
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger
