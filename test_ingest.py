"""Offline unit tests for the OpenSky ingestion pipeline."""

import json
from datetime import datetime, timedelta
from pathlib import Path

import requests

import ingest

SAMPLE_STATE = [
    "abc123",
    "GA401",
    "Indonesia",
    1700000000,
    1700000000,
    106.8,
    -6.1,
    10000,
    False,
    220.0,
    90.0,
    0.0,
    None,
    10100,
    "1234",
    False,
    0,
    None,
]


class FakeResponse:
    def __init__(self, status_code=200, payload=None, headers=None, text=""):
        self.status_code = status_code
        self._payload = payload if payload is not None else {}
        self.headers = headers or {}
        self.text = text

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")


def test_normalize_states_skips_rows_without_coordinates():
    payload = {
        "time": 1700000000,
        "states": [
            SAMPLE_STATE,
            ["def456", None, "Indonesia", 1700000000, 1700000000, None, None, None,
             False, None, None, None, None, None, None, False, 0, None],
        ],
    }

    rows = ingest.normalize_states(payload)

    assert len(rows) == 1
    assert rows[0]["icao24"] == "abc123"
    assert rows[0]["snapshot_time"] == 1700000000
    assert rows[0]["region"] == "southeast-asia"
    assert rows[0]["longitude"] == 106.8
    assert rows[0]["latitude"] == -6.1


def test_write_jsonl_roundtrip(tmp_path: Path):
    rows = [{"icao24": "abc123", "velocity": 220.0}]
    path = ingest.write_jsonl(rows, tmp_path / "sample.jsonl")

    loaded = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    assert loaded == rows


def test_auth_uses_cached_token(monkeypatch):
    posts = []

    class TokenResponse(FakeResponse):
        def json(self):
            return {"access_token": "tok", "expires_in": 1800}

    def fake_post(url, data, timeout):
        posts.append((url, data))
        return TokenResponse()

    monkeypatch.setattr(requests, "post", fake_post)
    auth = ingest.OpenSkyAuth("cid", "secret")

    assert auth.headers()["Authorization"] == "Bearer tok"
    assert auth.headers()["Authorization"] == "Bearer tok"
    assert len(posts) == 1
    assert posts[0][1]["grant_type"] == "client_credentials"


def test_fetch_snapshot_sends_bbox_and_bearer(monkeypatch):
    captured = {}

    def fake_get(self, url, **kwargs):
        captured["url"] = url
        captured.update(kwargs)
        return FakeResponse(payload={"time": 1700000000, "states": []})

    monkeypatch.setattr(requests.Session, "get", fake_get)
    auth = ingest.OpenSkyAuth("cid", "secret")
    auth._token = "tok"
    auth._expires_at = datetime.now() + timedelta(hours=1)

    payload = ingest.fetch_snapshot(requests.Session(), auth)

    assert payload["time"] == 1700000000
    assert captured["url"] == ingest.API_URL
    assert captured["headers"]["Authorization"] == "Bearer tok"
    assert captured["params"]["lamin"] == -11.0
    assert captured["params"]["lomax"] == 141.0
    assert captured["params"]["extended"] == 1


def test_fetch_snapshot_retries_on_rate_limit(monkeypatch):
    responses = [
        FakeResponse(429, headers={"X-Rate-Limit-Retry-After-Seconds": "1"}),
        FakeResponse(200, payload={"time": 1, "states": []}),
    ]

    def fake_get(self, url, **kwargs):
        return responses.pop(0)

    monkeypatch.setattr(requests.Session, "get", fake_get)
    monkeypatch.setattr(ingest.time, "sleep", lambda seconds: None)

    payload = ingest.fetch_snapshot(requests.Session(), ingest.OpenSkyAuth())

    assert payload["time"] == 1
    assert responses == []


def test_collect_rows_stops_when_target_reached(monkeypatch):
    monkeypatch.setattr(
        ingest,
        "fetch_snapshot",
        lambda *args, **kwargs: {"time": 1, "states": [SAMPLE_STATE]},
    )
    monkeypatch.setattr(ingest.time, "sleep", lambda seconds: None)

    rows = ingest.collect_rows(
        session=requests.Session(),
        auth=ingest.OpenSkyAuth(),
        snapshots=10,
        interval=0,
        min_rows=2,
    )

    assert len(rows) == 2


def test_fetch_snapshot_supports_historical_time(monkeypatch):
    captured = {}

    def fake_get(self, url, **kwargs):
        captured.update(kwargs)
        return FakeResponse(payload={"time": 1700000000, "states": []})

    monkeypatch.setattr(requests.Session, "get", fake_get)

    ingest.fetch_snapshot(requests.Session(), ingest.OpenSkyAuth(), time_secs=1700000000)

    assert captured["params"]["time"] == 1700000000


def test_historical_timestamps_stay_inside_one_hour():
    times = ingest.historical_timestamps(1000, now=1700000000)

    assert times[0] == 1700000000
    assert min(times) >= 1700000000 - 3600
    assert len(times) <= 721
    assert times[1] - times[0] == -5
