import os
from datetime import datetime, timedelta, date
from typing import Generator

import boto3
from boto3 import Session


def get_max_workers() -> int:
    return int(os.environ.get("MAX_WORKERS", 3))


def get_session(session: Session = None) -> boto3.Session:
    if session:
        return session
    else:
        return boto3.session.Session()


def get_yesterday() -> str:
    return str(date.today() - timedelta(1))


def get_day_back(back: int, relative_day: date = date.today()) -> str:
    return str(relative_day - timedelta(back))


def date_add(day: str, days_to_add: int) -> str:
    return str(datetime.strptime(day, "%Y-%m-%d") + timedelta(days_to_add))[:10]


def get_last_finished_week(day_str: str = None) -> str:
    if day_str and datetime.strptime(day_str, "%Y-%m-%d").weekday() == 6:
        return date_add(day_str, -6)
    day = datetime.strptime(day_str, "%Y-%m-%d").date() if day_str else date.today()
    return get_day_back(day.weekday() + 7, day)


def get_last_finished_month(day_str: str = None) -> str:
    if day_str and day_str == get_last_day_of_month(day_str[:7]):
        return day_str[:7]
    day = datetime.strptime(day_str, "%Y-%m-%d").date() if day_str else date.today()
    return get_day_back(day.day, day)[:7]


def get_last_day_of_month(month: str) -> str:
    cur_day = datetime.strptime(f"{month}-28", "%Y-%m-%d")
    next_day = cur_day + timedelta(days=1)
    while next_day.day > 1:
        cur_day = next_day
        next_day = cur_day + timedelta(days=1)
    return str(cur_day)[:10]


def days_range(from_day: str, to_day: str) -> Generator[str, None, None]:
    current_day = datetime.strptime(from_day, "%Y-%m-%d")
    yield str(current_day)[:10]
    last_date = datetime.strptime(to_day, "%Y-%m-%d")
    while current_day != last_date:
        current_day = current_day + timedelta(days=1)
        yield str(current_day)[:10]


def weeks_range(from_day: str, to_day: str) -> Generator[str, None, None]:
    current_day = datetime.strptime(from_day, "%Y-%m-%d")
    if current_day.weekday() > 0:
        current_day += timedelta(days=7 - current_day.weekday())
    last_date = datetime.strptime(to_day, "%Y-%m-%d")
    if current_day < last_date:
        last_week_day = current_day + timedelta(days=6)
        while last_week_day <= last_date:
            yield str(current_day)[:10]
            current_day = current_day + timedelta(weeks=1)
            last_week_day = current_day + timedelta(days=6)


def month_range(from_day: str, to_day: str) -> Generator[str, None, None]:
    """
    Generates a range of months between two dates.

    Args:
        from_day (str): The start date in the format 'YYYY-MM-DD'.
        to_day (str): The end date in the format 'YYYY-MM-DD'.

    Yields:
        str: A string representing the first day of each month in the range, in the format 'YYYY-MM'.

    Example:
        list(month_range('2023-01-15', '2023-05-10'))
        # Output: ['2023-02', '2023-03', '2023-04', '2023-05']
    """
    from_date = datetime.strptime(from_day, "%Y-%m-%d")
    if from_date.day > 1:
        from_date = _get_next_month(from_date)
    to_date = datetime.strptime(to_day, "%Y-%m-%d")
    last_month = to_date.replace(day=1)
    while from_date < last_month:
        yield str(from_date)[:7]
        from_date += timedelta(days=1)
        from_date = _get_next_month(from_date)

    if to_date.month != (to_date + timedelta(days=1)).month:
        yield str(to_date.replace(day=1))[:7]


def _get_next_month(the_date: datetime) -> datetime:
    while the_date.day > 1:
        the_date += timedelta(days=1)
    return the_date
