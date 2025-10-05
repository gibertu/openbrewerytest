"""Unit tests for Bronze extraction logic."""

import sys
from pathlib import Path
import pytest
from requests.exceptions import RequestException


ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT_DIR / "scripts"))

import bronze  


def test_fetch_breweries_paginates_until_empty(monkeypatch):
    """Ensure pagination stops when the API returns an empty page."""
    
    payloads = [
        [{"id": "1", "name": "Brew One"}],
        [{"id": "2", "name": "Brew Two"}],
        [],
    ]
    call_count = {"value": 0}

    class DummyResponse:
        def __init__(self, data):
            self._data = data

        def json(self):
            return self._data

        def raise_for_status(self):
            return None

    class DummySession:
        def get(self, *args, **kwargs):
            payload = payloads[call_count["value"]]
            call_count["value"] += 1
            return DummyResponse(payload)

        def close(self):
            call_count["closed"] = True

    call_count["closed"] = False
    monkeypatch.setattr(bronze, "_build_session", lambda: DummySession())

    breweries = bronze.fetch_breweries(per_page=1)

    assert [item["id"] for item in breweries] == ["1", "2"]
    assert call_count["value"] == 3  # two pages with data + one empty page
    assert call_count["closed"] is True


def test_fetch_breweries_propagates_request_errors(monkeypatch):
    """Verify request exceptions bubble up so Airflow can handle retries."""

    class FailingSession:
        def get(self, *args, **kwargs):
            raise RequestException("boom")

        def close(self):
            pass

    monkeypatch.setattr(bronze, "_build_session", lambda: FailingSession())

    with pytest.raises(RequestException):
        bronze.fetch_breweries(per_page=1)