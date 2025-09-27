"""Tests for record helper utilities."""

from __future__ import annotations

from src.core.record_utils import (
    build_record_context,
    extract_candidate_urls,
)


def test_build_record_context_filters_missing_values() -> None:
    row = {
        "BUSINESS_NAME": "Cafe Example",
        "ALTERNATE_NAME": " ",
        "LOCATION_CITY": "Seattle",
        "LOCATION_STATE_CODE": "NA",
        "LOCATION_COUNTRY": None,
    }

    context = build_record_context(row)

    assert context == {
        "BUSINESS_NAME": "Cafe Example",
        "LOCATION_CITY": "Seattle",
    }


def test_extract_candidate_urls_normalizes_and_deduplicates() -> None:
    row = {
        "WEBSITE": "example.com",
        "LINK": "https://example.com",
        "FACEBOOK_LINK": "facebook.com/cafe",
        "UNRELATED": "not a url",
    }

    urls = extract_candidate_urls(row, ["WEBSITE", "LINK", "FACEBOOK_LINK", "UNRELATED"])

    assert urls == [
        "https://example.com",
        "https://facebook.com/cafe",
    ]
