import logging
from random import randint
from typing import List

from boto3 import Session

from athena_utils import run_query, get_query_results
from utils import get_session, get_yesterday, get_temp_database_name
import concurrent.futures

_TEMP_TABLE_PREFIX = "temp_ctas"


def _get_temp_table_prefix(day: str) -> str:
    return f"{_TEMP_TABLE_PREFIX}_{day.replace('-', '_')}"


def _get_table_names(table_filter: str, session: Session) -> List[str]:
    query_results = get_query_results(
        f"SHOW TABLES IN {get_temp_database_name()} LIKE '{table_filter}'",
        session=session,
    )
    return [
        row["Data"][0]["VarCharValue"] for row in query_results["ResultSet"]["Rows"]
    ]


def generate_temp_table_name(day: str):
    return f"{get_temp_database_name()}.{_get_temp_table_prefix(day)}_{randint(100_000, 999_999)}"


def drop_temp_tables(event: dict) -> int:
    logging.info(
        f"START dropping temporary tables from database: {get_temp_database_name()}. Args: {event}"
    )
    session = get_session()
    day_fileter = event.get("day", get_yesterday())
    table_filter = _get_temp_table_prefix(day_fileter) + "*"
    table_names = _get_table_names(table_filter, session)
    logging.info(f"Filter: {table_filter} Found {len(table_names)} tables to drop")
    max_workers = event.get("max_workers", 3)
    logging.info(f"Max workers: {max_workers}")
    dropped_tables_num = 0
    errors = 0
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=max_workers, thread_name_prefix="drop_temp_table_"
    ) as thread_pool:
        futures = []
        for table_name in table_names:
            futures.append(thread_pool.submit(_drop_table, table_name, session))
        for f in concurrent.futures.as_completed(futures):
            if f.result():
                dropped_tables_num += 1
            else:
                errors += 1
            if (dropped_tables_num + errors) % 50 == 0:
                logging.info(f"Tables dropped: {dropped_tables_num}. Errs: {errors}")
        logging.info(f"Tables dropped: {dropped_tables_num}")
    logging.info(
        f"FINISHED dropping temporary tables. Dropped {dropped_tables_num} tables. Errs: {errors}"
    )
    return dropped_tables_num


def _drop_table(table_name: str, session: Session) -> bool:
    result = run_query(
        f"DROP TABLE {get_temp_database_name()}.{table_name}", session=session
    )
    return result.get("Status") == "SUCCEEDED"
