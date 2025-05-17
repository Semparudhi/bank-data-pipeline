import sys
from unittest.mock import MagicMock


mock_modules = ["awsglue", "awsglue.context", "awsglue.utils", "awsglue.job"]

for module in mock_modules:
    sys.modules[module] = MagicMock()
