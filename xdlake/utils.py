import json
import datetime


def timestamp(dt: datetime.datetime | None = None) -> int:
    dt = dt or datetime.datetime.now(datetime.timezone.utc)
    return int(dt.timestamp() * 1000)

def filename_for_version(version: int) -> str:
    return f"{version:020}.json"

class _JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, (datetime.date, datetime.datetime)):
            return o.isoformat()
        elif isinstance(o, bytes):
            return repr(o.decode("raw_unicode_escape", "backslashreplace"))
        return super().default(o)

def clien_version() -> str:
    try:
        from xdlake import __version__  # type: ignore
        return f"xdlake-{__version__.version}"
    except ImportError:
        return "xdlake-0.0.0"
