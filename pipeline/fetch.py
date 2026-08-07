#!/usr/bin/env python3
"""
fetch.py — the outside half of the data pipeline.

Fetches every enabled source in pipeline/sources.json, parses it into a clean
CSV of facts, and rewrites data/_status.json.

It does not filter, rank, score, or test eligibility, and it does not know that
IN_ sheets or holdings exist. If you are ever tempted to put a list of your
holdings in this file so it can trim the output, stop — that belongs in the
workbook, and putting it here is how "fetch prices" quietly grows a buyback
rule.

Standard library only, so the workflow needs no pip step.
"""

from __future__ import annotations

import csv
import io
import json
import os
import re
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(ROOT, "pipeline", "sources.json")
STATUS_PATH = os.path.join(ROOT, "data", "_status.json")

IST = timezone(timedelta(hours=5, minutes=30))
USER_AGENT = "finance-hub-pipeline/0.1"

RETRIES = 4
BACKOFF_SECONDS = 5  # doubles each attempt

# The Apps Script side identifies a wanted row by reading the first field of a
# line *without* CSV-parsing it, because parsing twelve thousand rows to keep
# five is wasteful. That only works if the first column never needs quoting.
SAFE_KEY = re.compile(r"^[A-Za-z0-9_.-]+$")


# ------------------------------------------------------------------ fetching

def fetch_text(url: str) -> str:
    last_error = None
    for attempt in range(RETRIES):
        try:
            request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(request, timeout=90) as response:
                return response.read().decode("utf-8", errors="replace")
        except Exception as exc:  # noqa: BLE001 - any transport failure retries
            last_error = exc
            if attempt < RETRIES - 1:
                time.sleep(BACKOFF_SECONDS * (2 ** attempt))
    raise RuntimeError(f"{url}: {last_error}")


# ------------------------------------------------------------------- parsers

AMFI_COLUMNS = [
    "scheme_code",
    "isin_growth",
    "isin_reinvest",
    "scheme_name",
    "nav",
    "nav_date",
    "amc",
    "category",
]

# NAVAll.txt interleaves two kinds of heading between data rows, and neither
# contains a semicolon: a fund-house name on its own line, and a scheme
# category that always begins Open / Close / Interval.
CATEGORY_START = re.compile(r"^(open|close|interval)\b", re.I)


def _iso_date(value: str) -> str:
    text = value.strip()
    if not text:
        return ""
    try:
        return datetime.strptime(text, "%d-%b-%Y").date().isoformat()
    except ValueError:
        # Land it unchanged rather than dropping a fact we merely failed to read.
        return text


def parse_amfi_nav(text: str):
    """AMFI's NAVAll.txt -> (columns, rows, skipped).

    The file is semicolon-delimited with fund-house and category headings
    interleaved between blocks of data rows, plus blank lines throughout. The
    headings are carried down onto each row: that is denormalising a fact the
    file already states, not a judgement about the row.
    """
    rows = []
    skipped = 0
    amc = ""
    category = ""

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        if ";" not in line:
            if CATEGORY_START.match(line):
                category = line
            else:
                amc = line
                category = ""
            continue

        parts = [part.strip() for part in line.split(";")]
        if len(parts) < 6:
            skipped += 1
            continue
        if parts[0].lower().startswith("scheme code"):
            continue  # the file's own header row

        code = parts[0]
        if not SAFE_KEY.match(code):
            skipped += 1
            continue

        rows.append(
            {
                "scheme_code": code,
                "isin_growth": parts[1],
                "isin_reinvest": parts[2],
                "scheme_name": parts[3],
                "nav": parts[4],          # may be "N.A." — landed as published
                "nav_date": _iso_date(parts[5]),
                "amc": amc,
                "category": category,
            }
        )

    return AMFI_COLUMNS, rows, skipped


PARSERS = {"amfi_nav": parse_amfi_nav}


# -------------------------------------------------------------------- output

def write_csv(path: str, columns, rows) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(columns)
    for row in rows:
        writer.writerow([row.get(column, "") for column in columns])
    with open(path, "w", encoding="utf-8", newline="") as handle:
        handle.write(buffer.getvalue())


def latest_date(rows, field: str) -> str:
    best = ""
    for row in rows:
        value = str(row.get(field, "")).strip()
        if len(value) == 10 and value[4] == "-" and value > best:
            best = value
    return best


def main() -> int:
    with open(CONFIG_PATH, encoding="utf-8") as handle:
        config = json.load(handle)

    now_utc = datetime.now(timezone.utc)
    results = {}
    failures = 0

    for key, spec in config.get("sources", {}).items():
        if not spec.get("enabled", True):
            continue

        entry = {
            "label": spec.get("label", key),
            "url": spec.get("url", ""),
            "out": spec.get("out", ""),
            "ok": False,
            "error": None,
            "fetched_at": now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "fetched_at_ist": now_utc.astimezone(IST).strftime("%Y-%m-%d %H:%M"),
            "row_count": 0,
            "skipped_rows": 0,
            "data_date": "",
        }

        try:
            parser = PARSERS.get(spec.get("parser", ""))
            if parser is None:
                raise RuntimeError(f"no parser named {spec.get('parser')!r}")

            columns, rows, skipped = parser(fetch_text(spec["url"]))
            if not rows:
                raise RuntimeError("parsed 0 rows — the source format probably changed")

            # Only written on success, so a bad fetch leaves yesterday's good
            # file in place rather than truncating it.
            write_csv(os.path.join(ROOT, spec["out"]), columns, rows)

            entry["ok"] = True
            entry["row_count"] = len(rows)
            entry["skipped_rows"] = skipped
            entry["data_date"] = latest_date(rows, "nav_date")

        except Exception as exc:  # noqa: BLE001 - one bad source must not stop the rest
            entry["error"] = str(exc)[:400]
            failures += 1

        results[key] = entry
        print(json.dumps({key: entry}, indent=2))

    os.makedirs(os.path.dirname(STATUS_PATH), exist_ok=True)
    with open(STATUS_PATH, "w", encoding="utf-8") as handle:
        json.dump(
            {
                "generated_at": now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "generated_at_ist": now_utc.astimezone(IST).strftime("%Y-%m-%d %H:%M"),
                "sources": results,
            },
            handle,
            indent=2,
            sort_keys=True,
        )
        handle.write("\n")

    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
