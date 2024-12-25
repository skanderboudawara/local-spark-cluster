import json
import logging.config
import logging.handlers
import pathlib


logger = logging.getLogger("compute")

config_file = pathlib.Path("/src/config/logger.json")
with open(config_file) as f_in:
    config = json.load(f_in)

logging.config.dictConfig(config)
logging.basicConfig(level="INFO")

