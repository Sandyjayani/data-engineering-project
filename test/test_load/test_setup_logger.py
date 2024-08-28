import pytest
import logging
import json
from src.load.setup_logger import setup_logger, JSONFormatter


def test_setup_logger_creation():
    logger = setup_logger("test_logger")
    assert isinstance(logger, logging.Logger)
    assert logger.name == "test_logger"
    assert logger.level == logging.DEBUG

def test_setup_logger_handler():
    logger = setup_logger("test_logger")
    assert len(logger.handlers) == 1
    handler = logger.handlers[0]
    assert isinstance(handler, logging.StreamHandler)

def test_setup_logger_formatter():
    logger = setup_logger("test_logger")
    handler = logger.handlers[0]
    assert isinstance(handler.formatter, JSONFormatter)

def test_json_formatter():
    formatter = JSONFormatter("%(asctime)s %(levelname)s %(name)s %(message)s %(filename)s %(funcName)s")
    record = logging.LogRecord("test_logger", logging.INFO, "test_file.py", 10, "Test message", None, None)
    formatted = formatter.format(record)
    log_dict = json.loads(formatted)

    assert "asctime" in log_dict
    assert log_dict["levelname"] == "INFO"
    assert log_dict["name"] == "test_logger"
    assert log_dict["message"] == "Test message"
    assert log_dict["filename"] == "test_file.py"
    assert "funcName" in log_dict


def test_multiple_logger_instances():
    logger1 = setup_logger("logger1")
    logger2 = setup_logger("logger2")

    assert logger1 != logger2
    assert logger1.name == "logger1"
    assert logger2.name == "logger2"

def test_duplicate_handler_prevention():
    logger = setup_logger("test_logger")
    initial_handler_count = len(logger.handlers)

    # Call setup_logger again with the same name
    setup_logger("test_logger")

    assert len(logger.handlers) == initial_handler_count