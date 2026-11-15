import logging
from datetime import datetime, timedelta, timezone
from random import randint
from typing import List, Optional

from boto3 import Session

from utils import get_session, get_temp_database_name
import concurrent.futures

_TEMP_TABLE_PREFIX = "temp_ctas"


_DEFAULT_MAX_AGE_HOURS = 24


def _get_temp_table_prefix(day: str) -> str:
    return f"{_TEMP_TABLE_PREFIX}_{day.replace('-', '_')}"


def _get_expired_table_names(
    max_age_hours: float, session: Session, now: Optional[datetime] = None
) -> List[str]:
    """Names of temp tables created more than max_age_hours ago.

    Selection is by Glue creation time and not by a date in the table name, since
    the name's middle part is not necessarily a date (e.g. temp_ctas_day_123456).
    """
    cutoff = (now or datetime.now(timezone.utc)) - timedelta(hours=max_age_hours)
    paginator = session.client("glue").get_paginator("get_tables")
    names = []
    try:
        for page in paginator.paginate(DatabaseName=get_temp_database_name()):
            for table in page["TableList"]:
                if (
                    table["Name"].startswith(_TEMP_TABLE_PREFIX)
                    and table["CreateTime"] < cutoff
                ):
                    names.append(table["Name"])
    except session.client("glue").exceptions.EntityNotFoundException:
        logging.warning(f"Temp database {get_temp_database_name()} not found")
    return names


def generate_temp_table_name(day: str):
    return f"{get_temp_database_name()}.{_get_temp_table_prefix(day)}_{randint(100_000, 999_999)}"


def drop_temp_tables(event: dict) -> int:
    logging.info(
        f"START dropping temporary tables from database: {get_temp_database_name()}. Args: {event}"
    )
    session = get_session()
    max_age_hours = event.get("max_age_hours", _DEFAULT_MAX_AGE_HOURS)
    table_names = _get_expired_table_names(max_age_hours, session)
    logging.info(
        f"Found {len(table_names)} temp tables older than {max_age_hours} hours to drop"
    )
    max_workers = event.get("max_workers", 3)
    logging.info(f"Max workers: {max_workers}")
    dropped_tables_num = 0
    errors = 0
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=max_workers, thread_name_prefix="drop_temp_table_"
    ) as thread_pool:
        futures = [
            thread_pool.submit(_drop_table, table_name, session)
            for table_name in table_names
        ]
        for f in concurrent.futures.as_completed(futures):
            if f.result():
                dropped_tables_num += 1
            else:
                errors += 1
            if (dropped_tables_num + errors) % 50 == 0:
                logging.info(f"Tables dropped: {dropped_tables_num}. Errs: {errors}")
    logging.info(
        f"FINISHED dropping temporary tables. Dropped {dropped_tables_num} tables. Errs: {errors}"
    )
    return dropped_tables_num


def _drop_table(table_name: str, session: Session) -> bool:
    # Only the Glue entry is removed: the tables are external, so S3 data is kept.
    try:
        session.client("glue").delete_table(
            DatabaseName=get_temp_database_name(), Name=table_name
        )
        return True
    except Exception as e:
        logging.warning(f"Failed dropping temp table {table_name}: {e}")
        return False
