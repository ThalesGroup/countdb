import logging
import os
import time
from typing import Generator, Dict, Optional, Iterable, List

from boto3 import Session
from botocore.exceptions import ClientError

from s3_utils import get_bucket, list_s3_folder_keys, clear_s3_folder
from utils import get_session, get_current_day, get_temp_database_name

_SLEEP_AFTER_QUERY_SECONDS = 1
_DEFAULT_QUERY_TIMEOUT = 300


class QueryStats:
    def __init__(self):
        self.num_of_queries = 0
        self.data_scanned = 0

    def add_query(self, data_scanned: int):
        self.num_of_queries += 1
        self.data_scanned += data_scanned

    @property
    def total_data_scanned(self) -> int:
        return self.data_scanned

    @property
    def total_data_scanned_formatted(self) -> str:
        return _format_data_scanned(self.total_data_scanned)

    def __str__(self) -> str:
        return f"{self.total_data_scanned_formatted} ({self.num_of_queries} queries)"


def reset_sleep_time_seconds():
    global _SLEEP_AFTER_QUERY_SECONDS
    _SLEEP_AFTER_QUERY_SECONDS = 0


def run_query(
    query: str,
    query_stats: QueryStats = None,
    timeout: Optional[int] = None,
    session: Session = None,
    sleep_time_seconds: Optional[int] = None,
):
    if not timeout:
        timeout = _DEFAULT_QUERY_TIMEOUT
    result = {}
    client = get_session(session).client("athena")
    if "RESULT_REUSE" in os.environ:
        response = client.start_query_execution(
            QueryString=query,
            ResultConfiguration={
                "OutputLocation": f"s3://{get_bucket()}/{os.environ['ATHENA_LOGS']}"
            },
            WorkGroup=os.environ["WORKGROUP"],
            ResultReuseConfiguration={
                "ResultReuseByAgeConfiguration": {
                    "Enabled": True,
                    "MaxAgeInMinutes": 60,
                }
            },
        )
    else:
        response = client.start_query_execution(
            QueryString=query,
            ResultConfiguration={
                "OutputLocation": f"s3://{get_bucket()}/{os.environ['ATHENA_LOGS']}"
            },
            WorkGroup=os.environ["WORKGROUP"],
        )
    time.sleep(
        _SLEEP_AFTER_QUERY_SECONDS if sleep_time_seconds is None else sleep_time_seconds
    )
    execution_id = response["QueryExecutionId"]
    result["QueryExecutionId"] = response["QueryExecutionId"]
    sleep_in_interval = 10
    intervals = int(timeout / sleep_in_interval)
    for wait_index in range(intervals):
        stats = client.get_query_execution(QueryExecutionId=execution_id)
        status = stats["QueryExecution"]["Status"]["State"]
        if status in ["FAILED", "CANCELLED", "SUCCEEDED"]:
            result["Status"] = status
            if status == "SUCCEEDED":
                result["OutputLocation"] = stats["QueryExecution"][
                    "ResultConfiguration"
                ]["OutputLocation"]
                result["QueryExecutionSeconds"] = (
                    stats["QueryExecution"]["Statistics"]["EngineExecutionTimeInMillis"]
                    / 1000
                )
                data_scan_bytes = int(
                    stats["QueryExecution"]["Statistics"]["DataScannedInBytes"]
                )
                if query_stats:
                    query_stats.add_query(data_scan_bytes)
                result["DataScanned"] = _format_data_scanned(data_scan_bytes)
            if "StateChangeReason" in stats["QueryExecution"]["Status"]:
                result["StateChangeReason"] = stats["QueryExecution"]["Status"][
                    "StateChangeReason"
                ]
            return result
        if wait_index % 6 == 0 and wait_index > 0:
            logging.info("Waiting to query for %d minutes", wait_index / 6)
        time.sleep(sleep_in_interval)
    err_msg = f"Timeout of {timeout} seconds occurred. Canceling query execution. Query id: {execution_id}"
    logging.warning(err_msg)
    client.stop_query_execution(QueryExecutionId=execution_id)
    result["Status"] = "TIMEOUT"
    result["Error"] = err_msg
    return result


def get_query_results(
    query: str,
    query_stats: QueryStats = None,
    timeout: int = _DEFAULT_QUERY_TIMEOUT,
    session: Session = None,
):
    try:
        query_execution_result = run_query(query, query_stats, timeout, session)
        if query_execution_result["Status"] == "SUCCEEDED":
            client = get_session(session).client("athena")
            return client.get_query_results(
                QueryExecutionId=query_execution_result["QueryExecutionId"]
            )
        else:
            message = (
                str(query_execution_result["StateChangeReason"])
                if "StateChangeReason" in query_execution_result
                else str(query_execution_result)
            )
            raise Exception("Error running query: %s\nMessage: %s" % (query, message))
    except Exception as e:
        raise e


def _format_data_scanned(data_scanned_bytes: int) -> str:
    if data_scanned_bytes < 1_000:
        return str(data_scanned_bytes) + "B"
    elif data_scanned_bytes < 1_000_000:
        return str(data_scanned_bytes // 1_000) + "KB"
    elif data_scanned_bytes < 1_000_000_000:
        return str(data_scanned_bytes // 1_000_000) + "MB"
    else:
        return str(round(data_scanned_bytes / 1_000_000_000, 2)) + "GB"


def _get_query_results_paginator(
    query: str,
    query_stats: QueryStats = None,
    timeout: int = _DEFAULT_QUERY_TIMEOUT,
    session: Session = None,
):
    try:
        query_execution_result = run_query(
            query=query, timeout=timeout, query_stats=query_stats, session=session
        )
        if query_execution_result["Status"] == "SUCCEEDED":
            client = get_session(session).client("athena")
            paginator = client.get_paginator("get_query_results")
            return paginator.paginate(
                QueryExecutionId=query_execution_result["QueryExecutionId"]
            )
        else:
            message = (
                str(query_execution_result["StateChangeReason"])
                if "StateChangeReason" in query_execution_result
                else str(query_execution_result)
            )
            raise Exception("Error running query: %s\nMessage: %s" % (query, message))
    except Exception as e:
        raise e


def get_query_results_dict(
    query: str,
    query_stats: QueryStats = None,
    timeout: int = _DEFAULT_QUERY_TIMEOUT,
    session: Session = None,
) -> Iterable[Dict]:
    pages_it = _get_query_results_paginator(query, query_stats, timeout, session)
    return _paginate_query_results(pages_it)


def _paginate_query_results(pages) -> Generator[Dict, None, None]:
    columns = None
    for page in pages:
        row_it = iter(page["ResultSet"]["Rows"])
        if columns is None:
            header = next(row_it)["Data"]
            columns = [header[i]["VarCharValue"] for i in range(len(header))]
        for row in row_it:
            row_dict = {}
            for i, col in enumerate(columns):
                value = row["Data"][i]
                row_dict[col] = value["VarCharValue"] if value else None
            yield row_dict


def database_exists(database_name: str, session: Optional[Session] = None) -> bool:
    query_result = get_query_results_dict(
        "SELECT 1 AS col FROM information_schema.tables\n"
        f"WHERE table_schema = '{database_name}'\n"
        f"LIMIT 1",
        session,
    )
    for _ in query_result:
        return True
    return False


def run_ctas_query(
    query_name: str,
    sql: str,
    field: str,
    force: bool = False,
    query_stats: QueryStats = None,
    session: Session = None,
) -> List[str]:
    try:
        output_location = (
            f"{os.environ['ATHENA_LOGS']}/day={get_current_day()}/name={query_name}/"
        )
        current_keys = list_s3_folder_keys(output_location, session)
        if len(current_keys) > 0:
            if force:
                deleted = clear_s3_folder(output_location, session)
                logging.info(
                    f"Cleared {deleted} files from S3 folder: {output_location}"
                )
                current_keys = []
            else:
                logging.info(
                    f"Found {len(current_keys)} files in S3 folder: {output_location}"
                )

        if len(current_keys) == 0:
            athena_temp_table = f"{get_temp_database_name()}.temp_countdb_export_{query_name.replace('-', '_')}"
            ctas_sql = f"""CREATE TABLE {athena_temp_table} 
            WITH (external_location='s3://{get_bucket()}/{output_location}', 
                  bucketed_by=ARRAY['{field}'], bucket_count=1, 
                  format='JSON') AS {sql}"""
            run_query(
                f"DROP TABLE IF EXISTS {athena_temp_table}",
                query_stats,
                session=session,
            )
            logging.info("Executing CTAS query...")
            result = run_query(ctas_sql, query_stats, session=session)
            logging.info(
                f"✅ CTAS query succeeded. Output location: {output_location} Query ID: {result['QueryExecutionId']}"
            )
            current_keys = list_s3_folder_keys(output_location, session)
        return current_keys
    except ClientError as e:
        logging.error(f"❌ AWS ClientError: {e}")
        raise
    except Exception as e:
        logging.error(f"❌ CTAS query failed: {e}")
        raise
