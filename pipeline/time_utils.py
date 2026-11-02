"""Calendar operations independent of current wall clock."""
import calendar
from datetime import datetime

def month_start(value):
    return datetime.fromisoformat(str(value)).replace(day=1, hour=0, minute=0, second=0, microsecond=0)

def add_months(value, count):
    value = datetime.fromisoformat(str(value))
    absolute = value.year * 12 + value.month - 1 + count
    year, month = divmod(absolute, 12)
    month += 1
    return value.replace(year=year, month=month, day=min(value.day, calendar.monthrange(year, month)[1]))

def shift_to_month(value, target):
    value = datetime.fromisoformat(str(value))
    target = month_start(target)
    return value.replace(year=target.year, month=target.month,
                         day=min(value.day, calendar.monthrange(target.year, target.month)[1]))
