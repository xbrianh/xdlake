from datetime import datetime, timezone


def timestamp(dt: datetime | None = None) -> int:
    dt = dt or datetime.now(timezone.utc)
    return int(dt.timestamp() * 1000)


def filename_for_version(version: int) -> str:
    return f"{version:020}.json"
