import time
from pathlib import Path
from datetime import datetime, timezone

class Common:
    @staticmethod
    def convert_str_to_date(date_str):
        return datetime.strptime(date_str,"%Y%m%d")

    @staticmethod
    def convert_str_to_date_utc(date_str):
        return datetime.strptime(date_str,"%Y%m%d").astimezone(timezone.utc)

    @staticmethod
    def datetime_to_ms_epoch(dt):
        microseconds = time.mktime(dt.timetuple()) * 1000000 + dt.microsecond
        return int(round(microseconds / float(1000)))

    @staticmethod
    def get_root_dir() -> Path:
        return Path(__file__).parent.parent