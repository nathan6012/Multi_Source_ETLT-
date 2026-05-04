import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

import pytest
from unittest.mock import AsyncMock, patch
from extract.api_connect import run_pipeline


def fake_response_1():
  return {
        "data": [{"id": "pi_1"}, {"id": "pi_2"}],
        "has_more": True
    }


def fake_response_2():
  return {
        "data": [{"id": "pi_3"}],
        "has_more": False
    }


@pytest.mark.asyncio
@patch("extract.api_connect.save_checkpoint")
@patch("extract.api_connect.load_checkpoint", return_value=None)
@patch("extract.api_connect.fetch", new_callable=AsyncMock)
async def test_run_pipeline(fetch_mock, load_mock, save_mock):

  fetch_mock.side_effect = [
        fake_response_1(),
        fake_response_2()
    ]

  result = await run_pipeline("test_key")

  assert len(result) == 3
  assert fetch_mock.call_count == 2
  save_mock.assert_called_with("pi_3")