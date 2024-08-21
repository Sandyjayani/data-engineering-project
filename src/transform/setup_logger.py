import logging
import json

"""
Aim: 
Set up and return a logger with JSON formatting
parameter: name 
(differnt logging instances but still in the same log group)
output: a logger, could be used in the other functions 
"""


class JSONFormatter(logging.Formatter):
    def format(self, record):
        log_obj = {
            "asctime": self.formatTime(record, self.datefmt),
            "levelname": record.levelname,
            "name": record.name,
            "message": record.getMessage(),
            "filename": record.filename,
            "funcName": record.funcName,
        }
        return json.dumps(log_obj)


def setup_logger(name: str):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # Avoid duplicate logs
    if not logger.handlers:
        json_handler = logging.StreamHandler()
        formatter = JSONFormatter(
            "%(asctime)s %(levelname)s %(name)s %(message)s "
            + "%(filename)s %(funcName)s"
        )
        json_handler.setFormatter(formatter)
        logger.addHandler(json_handler)

    # print(dir(logger.handlers[0].formatter))

    return logger
