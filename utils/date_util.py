from datetime import datetime
from zoneinfo import ZoneInfo


def get_current_pst_time() -> datetime:
    return datetime.now(tz=ZoneInfo("America/Los_Angeles"))