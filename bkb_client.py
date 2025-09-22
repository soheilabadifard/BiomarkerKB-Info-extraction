"""Thin client for interacting with BiomarkerKB HTTP API."""
from __future__ import annotations

import io
import json
from dataclasses import dataclass
from typing import Callable, Dict, Iterable, Optional

import pandas as pd
import requests

API_BASE = "https://api.biomarkerkb.org"
SEARCH_PATH = "/biomarker/search"
DOWNLOAD_PATH = "/data/list_download"


class BiomarkerKBError(RuntimeError):
    """Raised when the BiomarkerKB API request cannot be satisfied."""


@dataclass(frozen=True)
class ListRequest:
    """Parameters that drive creation of temporary server-side lists."""

    payload: Dict[str, object]
    description: str


def _safe_json(response: requests.Response) -> Dict[str, object]:
    """Parse JSON responses while surfacing malformed payloads as rich errors."""
    try:
        return response.json()
    except ValueError as exc:  # server returned HTML/text instead of JSON
        snippet = response.text[:500]
        raise BiomarkerKBError(
            "Non-JSON payload received from BiomarkerKB search endpoint. "
            "Status code: %s. Body snippet: %r" % (response.status_code, snippet)
        ) from exc


def create_list(list_request: ListRequest, timeout: int = 60) -> Optional[str]:
    """Create a BiomarkerKB list and return its identifier."""
    url = f"{API_BASE}{SEARCH_PATH}"
    headers = {"Content-Type": "application/json", "Accept": "application/json"}

    try:
        response = requests.post(url, json=list_request.payload, headers=headers, timeout=timeout)
        response.raise_for_status()
    except requests.exceptions.RequestException as exc:
        raise BiomarkerKBError(
            f"Search request failed for {list_request.description!r}: {exc}"
        ) from exc

    data = _safe_json(response)
    list_id = data.get("list_id")
    if not list_id:
        raise BiomarkerKBError(
            f"BiomarkerKB search response for {list_request.description!r} did not contain a 'list_id'."
        )
    return str(list_id)


def _parse_csv(data: str) -> pd.DataFrame:
    """Convert CSV text into a DataFrame while catching parser edge cases."""
    buffer = io.StringIO(data)
    try:
        return pd.read_csv(buffer)
    except pd.errors.EmptyDataError:
        # The API explicitly told us there are no rows.
        return pd.DataFrame()
    except (pd.errors.ParserError, UnicodeDecodeError) as exc:
        raise BiomarkerKBError(
            f"CSV parsing failed with error: {exc}."
        ) from exc


def download_list(list_id: str, *, expect_label: str, timeout: int = 300) -> pd.DataFrame:
    """Download a previously created list and materialise it as a DataFrame."""
    url = f"{API_BASE}{DOWNLOAD_PATH}"
    headers = {"Content-Type": "application/json", "Accept": "text/csv"}
    payload = {
        "id": list_id,
        "download_type": "biomarker_list",
        "format": "csv",
        "compressed": False,
    }

    try:
        response = requests.post(url, json=payload, headers=headers, timeout=timeout)
        response.raise_for_status()
    except requests.exceptions.RequestException as exc:
        raise BiomarkerKBError(
            f"Data download failed for {expect_label!r}: {exc}"
        ) from exc

    if not response.text or len(response.text.splitlines()) <= 1:
        return pd.DataFrame()

    try:
        return _parse_csv(response.text)
    except BiomarkerKBError:
        # When CSV parsing fails, fall back to JSON download and convert manually.
        json_payload = {
            "id": list_id,
            "download_type": "biomarker_list",
            "format": "json",
            "compressed": False,
        }
        try:
            json_resp = requests.post(url, json=json_payload, headers=headers, timeout=timeout)
            json_resp.raise_for_status()
        except requests.exceptions.RequestException as exc:
            raise BiomarkerKBError(
                f"CSV parsing failed and JSON fallback also errored for {expect_label!r}: {exc}"
            ) from exc

        try:
            parsed = json.loads(json_resp.text)
        except json.JSONDecodeError as exc:
            snippet = json_resp.text[:500]
            raise BiomarkerKBError(
                "BiomarkerKB JSON fallback payload was malformed. "
                "Body snippet: %r" % snippet
            ) from exc

        if isinstance(parsed, list):
            return pd.DataFrame(parsed)

        raise BiomarkerKBError(
            "Unexpected JSON structure received from BiomarkerKB download endpoint."
        )


def ensure_complete_results(df: pd.DataFrame, *, page_hint: Optional[int] = None) -> None:
    """Emit a warning when the client-side row count suggests truncation."""
    if page_hint is None or df.empty:
        return
    if len(df) >= page_hint:
        print(
            "⚠️ Retrieved %s rows, which matches or exceeds the configured page size (%s). "
            "Results may be truncated. Consider adjusting the request parameters."
            % (len(df), page_hint)
        )


def _default_log(message: str) -> None:
    print(message)


def download_with_size_escalation(
    *,
    payload_factory: Callable[[Optional[int]], Dict[str, object]],
    description: str,
    expect_label: str,
    initial_size: Optional[int],
    max_attempts: int = 4,
    logger: Callable[[str], None] = _default_log,
) -> Optional[pd.DataFrame]:
    """Create a list and download it, retrying with larger page sizes when needed."""
    attempt_size = initial_size
    attempts = 0
    previous_row_count: Optional[int] = None

    while True:
        logger(
            "🔬 Creating a search list for %s (size=%s)..." % (description, attempt_size or "auto")
        )
        request = ListRequest(payload=payload_factory(attempt_size), description=description)
        try:python3 -m compileall Biomaerker_by_entity.py Biomarker_by_specimen.py Biomarker_by_recordtype.py bkb_client.py
            list_id = create_list(request)
        except BiomarkerKBError as exc:
            logger(f"  ❌ API request failed for {description!r}: {exc}")
            return None

        logger(f"📂 Downloading data for List ID: {list_id}...")
        try:
            df = download_list(list_id, expect_label=expect_label)
        except BiomarkerKBError as exc:
            logger(f"  ❌ Data download or parsing failed for {description!r}: {exc}")
            return None

        ensure_complete_results(df, page_hint=attempt_size)
        row_count = len(df)
        logger(f"  ✅ Retrieved {row_count} rows for {description!r}.")

        if attempt_size is None or row_count == 0:
            return df

        if row_count < attempt_size:
            return df

        if previous_row_count == row_count:
            logger(
                "  ⚠️ Received the same row count on consecutive attempts; assuming the dataset is complete."
            )
            return df

        attempts += 1
        if attempts >= max_attempts:
            logger(
                "  ⚠️ Reached the maximum number of size escalation attempts. "
                "Proceeding with the most recent download."
            )
            return df

        previous_row_count = row_count
        attempt_size *= 2 if attempt_size is not None else None
        logger(
            "  ⚠️ Row count matches the requested page size. Retrying with a larger size (%s)."
            % attempt_size
        )



def chunk(iterable: Iterable[str], size: int) -> Iterable[list[str]]:
    """Yield successive fixed-size chunks from *iterable*."""
    batch: list[str] = []
    for item in iterable:
        batch.append(item)
        if len(batch) == size:
            yield batch
            batch = []
    if batch:
        yield batch
