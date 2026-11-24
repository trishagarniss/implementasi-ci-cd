"""Ingest OpenSky state vectors for Southeast Asia into Azure Blob Storage.

Usage (local demo, no Azure upload):
    python ingest.py --snapshots 3 --interval 5 --min-rows 0 --no-upload

Usage (GitHub Actions, historical batch):
    python ingest.py --historical --snapshots 240 --min-rows 0
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

API_URL = "https://opensky-network.org/api/states/all"
TOKEN_URL = (
    "https://auth.opensky-network.org/auth/realms/opensky-network/"
    "protocol/openid-connect/token"
)
BBOX_SE_ASIA = {"lamin": -11.0, "lomin": 95.0, "lamax": 20.0, "lomax": 141.0}

STATE_FIELDS = [
    "icao24",
    "callsign",
    "origin_country",
    "time_position",
    "last_contact",
    "longitude",
    "latitude",
    "baro_altitude",
    "on_ground",
    "velocity",
    "true_track",
    "vertical_rate",
    "sensors",
    "geo_altitude",
    "squawk",
    "spi",
    "position_source",
    "category",
]

LOG = logging.getLogger("ingest")


class OpenSkyAuth:
    """OAuth2 client-credentials token manager for the OpenSky REST API."""

    def __init__(self, client_id: str | None = None, client_secret: str | None = None):
        self.client_id = client_id
        self.client_secret = client_secret
        self._token: str | None = None
        self._expires_at: datetime | None = None

    @classmethod
    def from_env(cls) -> "OpenSkyAuth":
        return cls(
            os.getenv("OPENSKY_CLIENT_ID"),
            os.getenv("OPENSKY_CLIENT_SECRET"),
        )

    @property
    def enabled(self) -> bool:
        return bool(self.client_id and self.client_secret)

    def get_token(self) -> str:
        if not self.enabled:
            raise RuntimeError("OPENSKY_CLIENT_ID and OPENSKY_CLIENT_SECRET are required")
        if self._token and self._expires_at and datetime.now() < self._expires_at:
            return self._token
        response = requests.post(
            TOKEN_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            },
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        self._token = data["access_token"]
        expires_in = int(data.get("expires_in", 1800))
        self._expires_at = datetime.now() + timedelta(seconds=max(expires_in - 30, 30))
        return self._token

    def headers(self) -> dict:
        if not self.enabled:
            return {}
        return {"Authorization": f"Bearer {self.get_token()}"}

    def invalidate(self) -> None:
        self._token = None
        self._expires_at = None


def normalize_states(payload: dict, region: str = "southeast-asia") -> list[dict]:
    """Flatten the OpenSky array response into one dict per state vector."""
    snapshot_time = payload.get("time")
    ingested_at = (
        datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    )
    rows: list[dict] = []
    for state in payload.get("states") or []:
        values = dict(zip(STATE_FIELDS, state))
        if values.get("longitude") is None or values.get("latitude") is None:
            continue
        row = {
            "snapshot_time": snapshot_time,
            "ingested_at": ingested_at,
            "region": region,
        }
        row.update(values)
        rows.append(row)
    return rows


def historical_timestamps(count: int, now: int | None = None, step: int = 5) -> list[int]:
    """Return timestamps at 5s resolution inside the API's one-hour history window."""
    if count < 1:
        return []
    current = int(now if now is not None else time.time())
    newest = current - (current % step)
    oldest = current - 3600
    return [t for t in (newest - i * step for i in range(count)) if t >= oldest]


def fetch_snapshot(
    session: requests.Session,
    auth: OpenSkyAuth,
    bbox: dict | None = None,
    timeout: int = 30,
    retries: int = 3,
    time_secs: int | None = None,
) -> dict:
    """Fetch one snapshot, retrying on rate limit, token expiry and network errors."""
    params = dict(bbox or BBOX_SE_ASIA)
    params["extended"] = 1
    if time_secs is not None:
        params["time"] = int(time_secs)

    for attempt in range(1, retries + 1):
        try:
            response = session.get(
                API_URL,
                params=params,
                headers=auth.headers(),
                timeout=timeout,
            )
        except requests.RequestException as exc:
            if attempt == retries:
                raise RuntimeError(f"OpenSky request failed: {exc}") from exc
            time.sleep(2**attempt)
            continue

        if response.status_code == 401 and auth.enabled:
            auth.invalidate()
            if attempt == retries:
                raise RuntimeError("OpenSky rejected the bearer token (401)")
            continue

        if response.status_code == 429:
            retry_after = response.headers.get("X-Rate-Limit-Retry-After-Seconds", "")
            wait = int(retry_after) if retry_after.isdigit() else 10
            if attempt == retries:
                raise RuntimeError("OpenSky rate limit reached (429)")
            time.sleep(min(wait, 60))
            continue

        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            raise RuntimeError(
                f"OpenSky HTTP {response.status_code}: {response.text[:200]}"
            ) from exc

        payload = response.json()
        if not isinstance(payload, dict):
            raise RuntimeError("Unexpected OpenSky response format")
        return payload

    raise RuntimeError("OpenSky request failed after retries")


def collect_rows(
    session: requests.Session,
    auth: OpenSkyAuth,
    snapshots: int,
    interval: float,
    bbox: dict | None = None,
    min_rows: int = 0,
    historical: bool = False,
) -> list[dict]:
    """Collect snapshots until the row target or the snapshot limit is reached."""
    if historical:
        timestamps = historical_timestamps(snapshots)
        if len(timestamps) < snapshots:
            LOG.warning("History window limited to %d snapshots", len(timestamps))
    else:
        timestamps = [None] * snapshots

    rows: list[dict] = []
    for index, time_secs in enumerate(timestamps, start=1):
        payload = fetch_snapshot(session, auth, bbox=bbox, time_secs=time_secs)
        batch = normalize_states(payload)
        rows.extend(batch)
        print(
            f"[snapshot {index}/{len(timestamps)}] +{len(batch)} rows | total={len(rows)}",
            flush=True,
        )
        if min_rows and len(rows) >= min_rows:
            print(f"[info] target {min_rows} rows reached", flush=True)
            break
        if not historical and index < len(timestamps) and interval > 0:
            time.sleep(interval)
    return rows


def write_jsonl(rows: list[dict], path: Path) -> Path:
    """Write rows as newline-delimited JSON."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as stream:
        for row in rows:
            stream.write(json.dumps(row, ensure_ascii=False) + "\n")
    return path


def upload_to_azure(
    local_path: Path,
    container_name: str,
    connection_string: str,
    prefix: str = "bronze/opensky",
) -> str:
    """Upload the JSONL file to an Azure Blob Storage container."""
    try:
        from azure.core.exceptions import ResourceExistsError
        from azure.storage.blob import BlobServiceClient
    except ImportError as exc:
        raise RuntimeError("Install azure-storage-blob to upload data") from exc

    service = BlobServiceClient.from_connection_string(connection_string)
    container = service.get_container_client(container_name)
    try:
        container.create_container()
    except ResourceExistsError:
        pass

    blob_name = f"{prefix.strip('/')}/{local_path.name}"
    with local_path.open("rb") as stream:
        container.upload_blob(name=blob_name, data=stream, overwrite=True)
    return blob_name


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshots", type=int, default=100, help="maximum snapshots per run")
    parser.add_argument("--interval", type=float, default=5.0, help="seconds between snapshots")
    parser.add_argument("--min-rows", type=int, default=100000, help="target row count")
    parser.add_argument("--output-dir", default="data/raw", help="local output folder")
    parser.add_argument("--container", default="bronze", help="Azure blob container")
    parser.add_argument("--timeout", type=int, default=30, help="HTTP timeout in seconds")
    parser.add_argument(
        "--historical",
        action="store_true",
        help="request 5s-spaced timestamps from the last hour (OAuth required)",
    )
    parser.add_argument(
        "--no-upload",
        action="store_true",
        help="skip Azure upload (use for local testing)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    auth = OpenSkyAuth.from_env()
    if not auth.enabled:
        LOG.warning("No OpenSky credentials found. Using anonymous access (reduced quota).")
    if args.historical and not auth.enabled:
        raise RuntimeError("--historical requires OPENSKY_CLIENT_ID and OPENSKY_CLIENT_SECRET")

    started_at = datetime.now(timezone.utc)
    rows = collect_rows(
        session=requests.Session(),
        auth=auth,
        snapshots=args.snapshots,
        interval=args.interval,
        min_rows=args.min_rows,
        historical=args.historical,
    )
    if not rows:
        raise RuntimeError("No state vectors collected")

    if len(rows) < args.min_rows:
        LOG.warning("Target %d rows not reached; got %d", args.min_rows, len(rows))

    output_path = write_jsonl(
        rows,
        Path(args.output_dir) / f"opensky_{started_at:%Y%m%dT%H%M%SZ}.jsonl",
    )
    print(f"[ok] wrote {len(rows)} rows to {output_path}", flush=True)

    if not args.no_upload:
        connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING", "")
        if not connection_string:
            raise RuntimeError(
                "AZURE_STORAGE_CONNECTION_STRING is required (or use --no-upload)"
            )
        blob_name = upload_to_azure(output_path, args.container, connection_string)
        print(f"[ok] uploaded to {args.container}/{blob_name}", flush=True)

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        LOG.error("%s", exc)
        raise SystemExit(1)
