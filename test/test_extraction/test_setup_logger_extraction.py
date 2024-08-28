from src.extraction.setup_logger import setup_logger
import pytest
import unittest
import logging
from pythonjsonlogger import jsonlogger
from io import StringIO


class TestLoggerSetup(unittest.TestCase):
    @pytest.mark.it("Test if logger has correct level")
    def test_logger_has_correct_level(self):
        logger = setup_logger("Utility Functions")
        self.assertEqual(logger.level, logging.DEBUG)

    @pytest.mark.it("Test if there is only 1 handler")
    def test_only_one_handler(self):
        logger1 = setup_logger("Utility Functions")
        self.assertEqual(len(logger1.handlers), 1)
        logger2 = setup_logger("Utility Functions")
        self.assertEqual(len(logger1.handlers), 1)

    @pytest.mark.it("Test if the output is correct in each field")
    def test_if_correct_output_each_field(self):  # in doubt
        logger = setup_logger("test_logger")

        log_output = StringIO()
        test_handler = logging.StreamHandler(log_output)
        formatter = jsonlogger.JsonFormatter(
            "%(asctime)s %(levelname)s %(name)s %(message)s "
            + "%(filename)s %(funcName)s"
        )
        test_handler.setFormatter(formatter)
        logger.addHandler(test_handler)

        logger.info("Testing", extra={"table": "test_table"})
        log_output.seek(0)
        log_message = log_output.getvalue()

        self.assertIn("asctime", log_message)
        self.assertIn('"levelname": "INFO"', log_message)
        self.assertIn('"name": "test_logger"', log_message)
        self.assertIn('"message": "Testing"', log_message)
        self.assertIn('"funcName": "test_if_correct_output_each_field"', log_message)
        self.assertIn('"table": "test_table"', log_message)
