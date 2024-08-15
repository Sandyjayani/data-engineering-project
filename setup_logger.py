import logging
from pythonjsonlogger import jsonlogger
'''
Aim: 
Set up and return a logger with JSON formatting
parameter: name 
(differnt logging instances but still in the same log group)
output: a logger, could be used in the other functions 
'''
def setup_logger(name: str):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # Avoid duplicate logs
    if not logger.handlers:
        json_handler = logging.StreamHandler()
        formatter = jsonlogger.JsonFormatter(
                    '%(asctime)s %(levelname)s %(name)s %(message)s '+
                    '%(filename)s %(funcName)s'
                    )
        json_handler.setFormatter(formatter)
        logger.addHandler(json_handler)

    #print(dir(logger.handlers[0].formatter))
        
    return logger