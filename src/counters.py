import concurrent.futures
import logging
from datetime import datetime
from typing import Dict

from boto3 import Session

from athena_utils import QueryStats, get_query_results
from counters_utils import get_existing_data, BasCountersCreator
from datasets import (
    get_datasets,
    _Counter,
    get_dataset,
    replace_sql_place_holders,
    create_dataset_temp_tables,
    build_exists_query,
)
from table_creator import get_table_creator
from utils import days_range, get_yesterday, get_session, get_max_workers


def collect(
    from_day: str = None,
    to_day: str = None,
    dataset_name: str = None,
    override: bool = False,
    counter_id: int = None,
) -> Dict:
    """
    Collects daily counters for a specified date range and dataset.

    Args:
        from_day (str, optional): The start date for the collection in the format 'YYYY-MM-DD'. Defaults to yesterday.
        to_day (str, optional): The end date for the collection in the format 'YYYY-MM-DD'. Defaults to yesterday.
        dataset_name (str, optional): The name of the dataset to collect counters for. If None, collects for all datasets.
        override (bool, optional): If True, overrides existing data. Defaults to False.
        counter_id (int, optional): The ID of the counter to collect. If None, collects all counters.

    Returns:
        Dict: A dictionary containing the result of the collection operation, including the number of successful collections,
              the number of existing data points, the duration of the operation, and any errors encountered.
    """
    start_time = datetime.now()
    if from_day is None and from_day is None:
        from_day = get_yesterday()
        to_day = get_yesterday()
    if from_day > to_day:
        raise Exception("Start day must be before end day")
    session = get_session()
    if dataset_name:
        get_dataset(dataset_name)  # verify dataset exists
    existing_data = (
        {}
        if override
        else get_existing_data(
            "daily_counters",
            from_day,
            to_day,
            dataset_name,
            counter_ids=[counter_id] if counter_id else None,
            session=session,
        )
    )
    errors = 0
    success = 0
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=get_max_workers(), thread_name_prefix="collect_"
    ) as thread_pool:
        futures = []
        exists = []
        for dataset in [dataset_name] if dataset_name else get_datasets(session).keys():
            get_dataset(dataset, session)
            for day in days_range(from_day, to_day):
                log_str = f"Dataset: {dataset}, Day: {day}, Counter ID: {str(counter_id) if counter_id else 'ALL'}"
                if dataset in existing_data and day in existing_data[dataset]:
                    exists.append(day)
                else:
                    futures.append(
                        thread_pool.submit(
                            _run_daily_collection_task, dataset, day, counter_id
                        )
                    )
        if len(exists) > 0:
            logging.info(
                f"Data already exists for {len(exists)} days. {log_str}. Days: {exists}"
            )
        for f in concurrent.futures.as_completed(futures):
            if f.result():
                success += 1
            else:
                errors += 1
    elapsed_time = datetime.now() - start_time
    result = {
        "operation": "collect",
        "status": "UP",
        "success": success,
        "exists": len(exists),
        "duration": str(elapsed_time),
    }
    if errors > 0:
        result["errors"] = errors
    return result


def _run_daily_collection_task(dataset: str, day: str, counter_id: int) -> bool:
    log_str = f"Dataset: {dataset}, Day: {day}, Counter ID: {str(counter_id) if counter_id else 'ALL'}"
    session = get_session()
    try:
        if _calc_prerequisites(dataset, day, session):
            logging.info(f"Collecting counters for {log_str}")
            get_table_creator("daily_counters").create_day(
                dataset, day, counter_id, session
            )
        return True
    except Exception as e:
        logging.exception(
            f"Error collecting data for {log_str}. Error message: {str(e)}"
        )
        return False


def _calc_prerequisites(dataset_name: str, day: str, session: Session) -> bool:
    for q in get_dataset(dataset_name, session).prerequisites:
        q = build_exists_query(q, day)
        result = get_query_results(q, session=session)
        if (
            len(result["ResultSet"]["Rows"]) != 2
            or result["ResultSet"]["Rows"][1]["Data"][0]["VarCharValue"] != "true"
        ):
            logging.info(f"Prerequisite check failed: {q}")
            return False
    return True


class DailyCountersCreator(BasCountersCreator):
    def get_sql(
        self,
        dataset: str,
        day: str,
        counter_id: int = None,
        session: Session = None,
        **kwargs,
    ) -> str:
        result = ""
        counters = get_dataset(dataset, session).counters
        temp_tables = kwargs["temp_tables"]
        for counter in [counters[counter_id]] if counter_id else counters.values():
            if len(result) > 0:
                result += f"UNION ALL"
            result += f"({self.get_counter_sql(counter, day, temp_tables)})"
        return result

    @staticmethod
    def get_counter_sql(
        counter: _Counter, day: str, temp_tables: Dict[str, str]
    ) -> str:
        return f"""SELECT {counter.id} AS counter_id,
  CAST(CASE WHEN CARDINALITY(int_key) > 0 THEN int_key[1] ELSE NULL END AS BIGINT) AS int_key1,
  CAST(CASE WHEN CARDINALITY(int_key) > 1 THEN int_key[2] ELSE NULL END AS BIGINT) AS int_key2,
  CAST(CASE WHEN CARDINALITY(str_key) > 0 THEN str_key[1] ELSE NULL END AS VARCHAR) AS str_key1,
  CAST(CASE WHEN CARDINALITY(str_key) > 1 THEN str_key[2] ELSE NULL END AS VARCHAR) AS str_key2,
  value,
  {counter.id} AS cnt_id
FROM ({replace_sql_place_holders(counter.sql, day, temp_tables)})
ORDER BY int_key1, int_key2, str_key1, str_key2"""

    def table_name(self):
        return "daily_counters"

    def table_folder(self):
        return "daily-counters"

    def partition_name(self) -> str:
        return "day"

    def _create_temp_tables(
        self,
        dataset_name: str,
        day: str,
        query_stats: QueryStats,
        session: Session = None,
    ) -> Dict[str, str]:
        dataset = get_dataset(dataset_name, session=session)
        return create_dataset_temp_tables(dataset, day, query_stats, session=session)

    def life_cycle_days(self) -> int:
        return 180
