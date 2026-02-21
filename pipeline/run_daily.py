#!/usr/bin/env python3
"""Daily JSON generator for notallbad country feeds.

Reliability-first pipeline:
- pulls RSS/Atom feeds from official sources
- filters to last 72 hours (UTC)
- keeps only numeric deltas/metrics or official approval milestones
- rejects obvious negative events
- deduplicates and keeps up to 10 items per country with category diversity
- validates JSON schema
- writes atomically (tmp + rename)
"""

from __future__ import annotations

import argparse
import html
import json
import logging
import os
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import feedparser
import requests
from dateutil import parser as dt_parser
from jsonschema import Draft202012Validator

ROOT_DIR = Path(__file__).resolve().parents[1]
CONFIG_DIR = Path(__file__).resolve().parent / "config"
OUTPUT_DIR = ROOT_DIR / "notallbad"
LOG_DIR = ROOT_DIR / "pipeline" / "logs"

MAX_ITEMS = 10
LOOKBACK_HOURS_DEFAULT = 72
REQUEST_TIMEOUT_SECONDS = 20
USER_AGENT = "politeia-data-pipeline/1.0"

COUNTRY_NAMES = {
    "usa": "USA",
    "germany": "Germany",
    "india": "India",
    "japan": "Japan",
    "uk": "UK",
    "france": "France",
    "italy": "Italy",
    "southkorea": "South Korea",
    "brazil": "Brazil",
}

ALLOWED_CATEGORIES = [
    "Economy",
    "Health",
    "Science",
    "Education",
    "Infrastructure",
    "Environment",
    "Technology",
    "Public Safety",
    "Energy",
    "Civic",
]

NEGATIVE_TERMS = {
    "killed",
    "dead",
    "death",
    "attack",
    "outbreak",
    "war",
    "crisis",
    "fraud",
    "terror",
    "homicide",
    "shooting",
    "explosion",
    "bankrupt",
    "bankruptcy",
    "recession",
    "collapse",
    "earthquake",
    "flood",
    "wildfire",
}

APPROVAL_TERMS = {
    "approved",
    "approval",
    "authorized",
    "authorised",
    "ratified",
    "adopted",
    "launched",
    "inaugurated",
    "certified",
}

METRIC_PATTERNS = [
    re.compile(r"[+\-]\d+(?:\.\d+)?\s?%"),
    re.compile(
        r"\d+(?:\.\d+)?\s?(?:%|percent|bps|basis points|points|million|billion|trillion|"
        r"usd|eur|gbp|jpy|inr|brl|krw|gw|mw|gwh|mwh|twh)",
        re.IGNORECASE,
    ),
    re.compile(r"\d[\d,]*(?:\.\d+)?"),
]

SCHEMA: dict[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "additionalProperties": False,
    "required": ["country", "date", "generated_at", "items"],
    "properties": {
        "country": {"type": "string"},
        "date": {"type": "string", "pattern": r"^\d{4}-\d{2}-\d{2}$"},
        "generated_at": {"type": "string", "format": "date-time"},
        "items": {
            "type": "array",
            "minItems": 0,
            "maxItems": MAX_ITEMS,
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": [
                    "category",
                    "headline",
                    "context",
                    "metric",
                    "source",
                    "source_name",
                    "timestamp",
                ],
                "properties": {
                    "category": {"type": "string", "enum": ALLOWED_CATEGORIES},
                    "headline": {"type": "string", "maxLength": 90},
                    "context": {"type": "string", "maxLength": 220},
                    "metric": {"type": "string"},
                    "source": {
                        "type": "string",
                        "pattern": r"^https?://",
                    },
                    "source_name": {"type": "string"},
                    "timestamp": {"type": "string", "format": "date-time"},
                },
            },
        },
    },
}

VALIDATOR = Draft202012Validator(SCHEMA)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_utc(dt_value: datetime) -> str:
    return dt_value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def setup_logging(run_id: str) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_path = LOG_DIR / f"run_{run_id}.log"

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(logging.INFO)

    formatter = logging.Formatter("%(asctime)sZ %(levelname)s %(message)s", datefmt="%Y-%m-%dT%H:%M:%S")

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)

    root_logger.addHandler(stream_handler)
    root_logger.addHandler(file_handler)


def prune_logs(keep: int = 30) -> None:
    if not LOG_DIR.exists():
        return
    logs = sorted(LOG_DIR.glob("run_*.log"))
    for old_path in logs[:-keep]:
        old_path.unlink(missing_ok=True)


def clean_text(raw_text: str | None) -> str:
    if not raw_text:
        return ""
    text = html.unescape(raw_text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def shorten(text: str, max_len: int) -> str:
    if len(text) <= max_len:
        return text
    if max_len <= 3:
        return text[:max_len]
    return text[: max_len - 3].rstrip() + "..."


def has_negative_terms(text: str) -> bool:
    lowered = text.lower()
    return any(re.search(rf"\b{re.escape(term)}\b", lowered) for term in NEGATIVE_TERMS)


def has_approval_milestone(text: str) -> bool:
    lowered = text.lower()
    return any(re.search(rf"\b{re.escape(term)}\b", lowered) for term in APPROVAL_TERMS)


def extract_metric(text: str) -> str | None:
    for pattern in METRIC_PATTERNS:
        match = pattern.search(text)
        if not match:
            continue
        start = max(0, match.start() - 28)
        end = min(len(text), match.end() + 28)
        snippet = clean_text(text[start:end])
        if snippet:
            return shorten(snippet, 120)
        return match.group(0)
    return None


def parse_entry_timestamp(entry: dict[str, Any]) -> datetime | None:
    for parsed_key in ("published_parsed", "updated_parsed", "created_parsed"):
        parsed_value = entry.get(parsed_key)
        if parsed_value:
            return datetime.fromtimestamp(time.mktime(parsed_value), tz=timezone.utc)

    for text_key in ("published", "updated", "created"):
        text_value = entry.get(text_key)
        if not text_value:
            continue
        try:
            parsed_dt = dt_parser.parse(text_value)
        except (TypeError, ValueError, OverflowError):
            continue
        if parsed_dt.tzinfo is None:
            parsed_dt = parsed_dt.replace(tzinfo=timezone.utc)
        return parsed_dt.astimezone(timezone.utc)

    return None


def is_recent(timestamp: datetime, now: datetime, lookback_hours: int) -> bool:
    return now - timedelta(hours=lookback_hours) <= timestamp <= now + timedelta(minutes=5)


def normalize_headline(headline: str) -> str:
    headline = headline.lower()
    headline = re.sub(r"[^a-z0-9\s]", " ", headline)
    headline = re.sub(r"\s+", " ", headline)
    return headline.strip()


def headline_tokens(headline: str) -> set[str]:
    return {token for token in normalize_headline(headline).split() if len(token) > 2}


def is_duplicate(candidate: dict[str, Any], selected: list[dict[str, Any]]) -> bool:
    cand_norm = normalize_headline(candidate["headline"])
    cand_tokens = headline_tokens(candidate["headline"])

    for previous in selected:
        prev_norm = normalize_headline(previous["headline"])
        if cand_norm == prev_norm:
            return True
        prev_tokens = headline_tokens(previous["headline"])
        if not cand_tokens or not prev_tokens:
            continue
        overlap = len(cand_tokens & prev_tokens)
        union = len(cand_tokens | prev_tokens)
        if union and (overlap / union) >= 0.8:
            return True
    return False


def classify_category(text: str, categories_config: dict[str, list[str]]) -> str:
    lowered = text.lower()
    for category in ALLOWED_CATEGORIES:
        keywords = categories_config.get(category, [])
        if any(keyword.lower() in lowered for keyword in keywords):
            return category
    return "Civic"


def source_name_for(feed_cfg: dict[str, Any], parsed_feed: Any, source_url: str) -> str:
    if feed_cfg.get("name"):
        return str(feed_cfg["name"])

    feed_title = clean_text(parsed_feed.get("feed", {}).get("title"))
    if feed_title:
        return feed_title

    hostname = urlparse(source_url).hostname or source_url
    return hostname


def validate_payload(payload: dict[str, Any]) -> None:
    errors = sorted(VALIDATOR.iter_errors(payload), key=lambda error: list(error.path))
    if not errors:
        return
    first_error = errors[0]
    path = ".".join(str(part) for part in first_error.path)
    raise ValueError(f"Schema validation failed at '{path}': {first_error.message}")


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    os.replace(tmp_path, path)


def pick_country_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    items_sorted = sorted(items, key=lambda item: item["timestamp"], reverse=True)

    deduped: list[dict[str, Any]] = []
    for item in items_sorted:
        if is_duplicate(item, deduped):
            continue
        deduped.append(item)

    chosen: list[dict[str, Any]] = []
    used_categories: set[str] = set()

    for item in deduped:
        if item["category"] in used_categories:
            continue
        chosen.append(item)
        used_categories.add(item["category"])
        if len(chosen) >= MAX_ITEMS:
            return chosen

    for item in deduped:
        if item in chosen:
            continue
        chosen.append(item)
        if len(chosen) >= MAX_ITEMS:
            break

    return chosen


def country_keywords_for(country_code: str, feed_cfg: dict[str, Any]) -> list[str]:
    explicit = feed_cfg.get("country_keywords")
    if explicit:
        return [str(value).lower() for value in explicit]

    country_name = COUNTRY_NAMES[country_code]
    defaults = [country_name.lower(), country_code.lower()]
    if country_code == "usa":
        defaults.extend(["united states", "u.s.", "american"])
    if country_code == "uk":
        defaults.extend(["united kingdom", "u.k.", "britain", "british"])
    if country_code == "southkorea":
        defaults.extend(["south korea", "republic of korea", "korean"])
    return defaults


def country_match(text: str, keywords: list[str]) -> bool:
    lowered = text.lower()
    return any(keyword in lowered for keyword in keywords)


def fetch_feed(feed_cfg: dict[str, Any]) -> Any:
    url = str(feed_cfg["url"])
    response = requests.get(
        url,
        timeout=REQUEST_TIMEOUT_SECONDS,
        headers={"User-Agent": USER_AGENT},
    )
    response.raise_for_status()
    return feedparser.parse(response.content)


def entry_to_item(
    *,
    entry: dict[str, Any],
    country_code: str,
    now: datetime,
    lookback_hours: int,
    feed_cfg: dict[str, Any],
    parsed_feed: Any,
    categories_config: dict[str, list[str]],
) -> dict[str, Any] | None:
    timestamp = parse_entry_timestamp(entry)
    if not timestamp or not is_recent(timestamp, now, lookback_hours):
        return None

    headline = clean_text(entry.get("title"))
    context = clean_text(entry.get("summary") or entry.get("description") or entry.get("title"))

    if not headline:
        return None

    headline = shorten(headline, 90)
    context = shorten(context, 220)

    link = clean_text(entry.get("link"))
    if not link.startswith("http"):
        return None

    combined_text = f"{headline} {context}".strip()
    if has_negative_terms(combined_text):
        return None

    keywords = country_keywords_for(country_code, feed_cfg)
    if keywords and not country_match(combined_text, keywords):
        return None

    metric = extract_metric(combined_text)
    approval = has_approval_milestone(combined_text)
    if not metric and not approval:
        return None
    if not metric and approval:
        metric = "official approval milestone"

    category = classify_category(combined_text, categories_config)
    source_name = source_name_for(feed_cfg, parsed_feed, link)

    return {
        "category": category,
        "headline": headline,
        "context": context,
        "metric": metric,
        "source": link,
        "source_name": source_name,
        "timestamp": iso_utc(timestamp),
    }


def empty_payload(country_code: str, generated_at: datetime) -> dict[str, Any]:
    return {
        "country": COUNTRY_NAMES[country_code],
        "date": generated_at.strftime("%Y-%m-%d"),
        "generated_at": iso_utc(generated_at),
        "items": [],
    }


def generate_for_country(
    *,
    country_code: str,
    feeds: list[dict[str, Any]],
    categories_config: dict[str, list[str]],
    now: datetime,
    lookback_hours: int,
) -> tuple[dict[str, Any], dict[str, int]]:
    stats = {
        "feeds_total": len(feeds),
        "feeds_ok": 0,
        "feeds_failed": 0,
        "entries_seen": 0,
        "items_kept": 0,
    }

    items: list[dict[str, Any]] = []

    for feed_cfg in feeds:
        feed_name = str(feed_cfg.get("name", feed_cfg.get("url", "unknown")))
        try:
            parsed_feed = fetch_feed(feed_cfg)
        except Exception as exc:
            stats["feeds_failed"] += 1
            logging.warning("country=%s feed_failed=%s reason=%s", country_code, feed_name, exc)
            continue

        stats["feeds_ok"] += 1

        for entry in parsed_feed.get("entries", []):
            stats["entries_seen"] += 1
            item = entry_to_item(
                entry=entry,
                country_code=country_code,
                now=now,
                lookback_hours=lookback_hours,
                feed_cfg=feed_cfg,
                parsed_feed=parsed_feed,
                categories_config=categories_config,
            )
            if not item:
                continue
            items.append(item)

    chosen = pick_country_items(items)
    stats["items_kept"] = len(chosen)

    payload = empty_payload(country_code, now)
    payload["items"] = chosen
    validate_payload(payload)
    return payload, stats


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate daily notallbad country JSON files.")
    parser.add_argument(
        "--country",
        action="append",
        choices=sorted(COUNTRY_NAMES.keys()),
        help="Limit run to one or more country codes.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    now = utc_now()
    run_id = now.strftime("%Y%m%dT%H%M%SZ")
    setup_logging(run_id)

    feeds_config = load_json(CONFIG_DIR / "feeds.json")
    categories_config = load_json(CONFIG_DIR / "categories.json")

    lookback_hours = int(feeds_config.get("lookback_hours", LOOKBACK_HOURS_DEFAULT))
    feeds_by_country = feeds_config.get("countries", {})

    countries = args.country or list(COUNTRY_NAMES.keys())

    logging.info("run_id=%s countries=%s lookback_hours=%s", run_id, ",".join(countries), lookback_hours)

    for country_code in countries:
        country_feeds = feeds_by_country.get(country_code, [])
        payload, stats = generate_for_country(
            country_code=country_code,
            feeds=country_feeds,
            categories_config=categories_config,
            now=now,
            lookback_hours=lookback_hours,
        )

        output_path = OUTPUT_DIR / f"{country_code}.json"
        write_json_atomic(output_path, payload)

        logging.info(
            (
                "country=%s feeds_total=%d feeds_ok=%d feeds_failed=%d "
                "entries_seen=%d items_kept=%d output=%s"
            ),
            country_code,
            stats["feeds_total"],
            stats["feeds_ok"],
            stats["feeds_failed"],
            stats["entries_seen"],
            stats["items_kept"],
            output_path,
        )

    prune_logs(keep=30)
    logging.info("run_id=%s completed", run_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
