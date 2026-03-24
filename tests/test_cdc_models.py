from datetime import datetime, timezone

from cdc.src.models import parse_datetime


def test_parse_datetime_naive_string_as_utc():
    dt = parse_datetime("1970-01-01T00:00:00")
    assert dt.tzinfo is not None
    assert dt == datetime(1970, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


def test_parse_datetime_naive_datetime_as_utc():
    dt = parse_datetime(datetime(1970, 1, 1, 0, 0, 0))
    assert dt.tzinfo is not None
    assert dt == datetime(1970, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
