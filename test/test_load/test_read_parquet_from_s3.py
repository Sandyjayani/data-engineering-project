import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from botocore.exceptions import ClientError
from src.load.read_parquet_from_s3 import read_parquet_from_s3


