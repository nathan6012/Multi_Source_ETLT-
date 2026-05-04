import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

import pytest
from unittest.mock import AsyncMock, patch, MagicMock

from extract.database_connect import extract_from_db













