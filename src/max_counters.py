import concurrent.futures
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional, Dict

from boto3 import Session

from athena_utils import get_query_results
from counters import BasCountersCreator
from datasets import get_dataset, get_datasets
from table_creator import get_table_creator
from utils import (
    get_session,
    get_yesterday,
    get_last_finished_week,
    get_last_finished_month,
    get_max_workers,
)


class MaxCounters(BasCountersCreator, ABC):
    def get_sql(
        self,
        dataset: str,
        day: str,
        counter_id: int = None,
        session: Session = None,
        **kwargs,
    ) -> str:
        last_max_interval_dict = self._get_last_max_interval(
            day, dataset, counter_id, session
        )
        logging.info(f"Last max interval: {last_max_interval_dict}")
        last_max_interval = (
            last_max_interval_dict["last_interval"] if last_max_interval_dict else None
        )
        if last_max_interval is None:
            max_last_seen_sql = 0
        else:
            max_last_seen_sql = (
                f"{self._date_diff(f"'{last_max_interval}'", f"'{day}'")} + last_seen"
            )
        return f"""
WITH data AS (
  -- Find keys and max values for counter during the period
  WITH collction_data AS (
    SELECT int_key1, int_key2, str_key1, str_key2,
      MAX(value) AS value,
      ARRAY_AGG(value ORDER BY {self.partition_name()}) AS vals,
      ARRAY_AGG({self.partition_name()} ORDER BY {self.partition_name()}) AS intervals,
      MAX(IF(value > 0, {self.partition_name()})) AS last_seen_interval
    FROM  {self.database_name()}.{self.source_table()} 
    WHERE dataset = '{dataset}'
      AND {self.partition_name()} BETWEEN 
          {self._date_add(last_max_interval, 1) if last_max_interval else f"'{kwargs.get('from_date', '')}'"} 
          AND '{day}'
      AND counter_id = {counter_id}
    GROUP BY int_key1, int_key2, str_key1, str_key2)
  -- per key calc the prev value and prev max interval
  SELECT int_key1, int_key2, str_key1, str_key2, last_seen_interval, value, interval, prev_value,
    CASE WHEN prev_value IS NULL THEN NULL ELSE intervals[ARRAY_POSITION(vals, prev_value)] END AS prev_max_interval
  FROM collction_data
    CROSS JOIN LATERAL (SELECT intervals[ARRAY_POSITION(vals, value)] AS interval) 
    CROSS JOIN LATERAL (
    SELECT ARRAY_MAX(SLICE(vals, 1, ARRAY_POSITION(intervals, interval) - 1)) AS prev_value)),
-- query max data per key for the counter 
max_data AS (
   SELECT int_key1, int_key2, str_key1, str_key2, value, max_interval, prev_value, prev_max_interval, last_seen
   FROM {self.database_name()}.{self.table_name()}
   WHERE dataset = '{dataset}'
         AND {"False" if last_max_interval is None else f"{self.partition_name()} = '{last_max_interval}'"}
         AND counter_id = {counter_id})
-- use full join between the data and max data, to handle NULLs         
SELECT {counter_id} AS counter_id,
  {"int_key1" if last_max_interval_dict and last_max_interval_dict["int_key1"] else "data.int_key1"},
  {"int_key2" if last_max_interval_dict and last_max_interval_dict["int_key2"] else "data.int_key2"},
  {"str_key1" if last_max_interval_dict and last_max_interval_dict["str_key1"] else "data.str_key1"},
  {"str_key2" if last_max_interval_dict and last_max_interval_dict["str_key2"] else "data.str_key2"},  
  CASE 
    WHEN data.value IS NULL THEN max_data.value
    WHEN max_data.value IS NULL THEN data.value 
    ELSE GREATEST(max_data.value, data.value) 
  END AS value, 
  CASE 
    WHEN data.value IS NULL THEN max_data.max_interval 
    WHEN max_data.value IS NULL THEN data.interval
    WHEN data.value > max_data.value THEN data.interval
    ELSE max_data.max_interval
   END AS max_interval,
   CASE
     WHEN data.value IS NOT NULL AND data.value > 0 THEN {self._date_diff("data.last_seen_interval", f"'{day}'")}
     ELSE {max_last_seen_sql}
   END AS last_seen, 
   CASE
     WHEN max_data.value IS NULL OR data.prev_value > max_data.value THEN data.prev_value      
     WHEN data.value > max_data.value THEN max_data.value
     ELSE max_data.prev_value
   END AS prev_value,
   CASE
     WHEN max_data.value IS NULL OR data.prev_value > max_data.value THEN data.prev_max_interval
     WHEN data.value > max_data.value THEN max_data.max_interval
     ELSE max_data.prev_max_interval
   END AS prev_max_interval,
   '{day}' AS {self.partition_name()},
   '{dataset}' AS dataset
FROM max_data FULL OUTER JOIN data {self._get_join(last_max_interval_dict)} 
WHERE      
  data.value IS NOT NULL AND data.value > 0 
  OR (max_data.last_seen IS NOT NULL 
      AND {max_last_seen_sql} <= {self._max_not_seen_intervals()})
"""

    @staticmethod
    def _get_join(last_max_interval: Optional[Dict]) -> str:
        if last_max_interval is None:
            return "ON TRUE"
        else:
            result = ""
            for key in ["int_key1", "int_key2", "str_key1", "str_key2"]:
                if last_max_interval[key]:
                    result += f"{key},"
            if len(result) == 0:
                return "ON TRUE"
            else:
                return f"USING ({result[:-1]})"

    def _max_not_seen_intervals(self) -> int:
        raise NotImplementedError()

    def _get_last_max_interval(
        self, day: str, dataset: str, counter_id: int, session: Session = None
    ) -> Optional[Dict]:
        results = get_query_results(
            f"""
SELECT MAX({self.partition_name()}) AS interval, 
       ARBITRARY(int_key1) AS int_key1, ARBITRARY(int_key2) AS int_key2,
       ARBITRARY(str_key1) AS str_key1, ARBITRARY(str_key2) AS str_key2
FROM {self.full_table_name()}
WHERE {self.partition_name()} < '{day}'
      AND dataset = '{dataset}'
      AND counter_id = {counter_id}""",
            session=session,
        )

        data = results["ResultSet"]["Rows"][1]["Data"]
        if "VarCharValue" in data[0] and len(data[0]["VarCharValue"][0]) > 0:
            result = {
                "last_interval": data[0]["VarCharValue"],
                "int_key1": "VarCharValue" in data[1]
                and len(data[1]["VarCharValue"][0]) > 0,
                "int_key2": "VarCharValue" in data[2]
                and len(data[2]["VarCharValue"][0]) > 0,
                "str_key1": "VarCharValue" in data[3]
                and len(data[3]["VarCharValue"][0]) > 0,
                "str_key2": "VarCharValue" in data[4]
                and len(data[4]["VarCharValue"][0]) > 0,
            }
            return result
        else:
            return None

    @staticmethod
    def external_table_columns() -> Dict[str, str]:
        columns = BasCountersCreator.external_table_columns().copy()
        columns["max_interval"] = "string"
        columns["last_seen"] = "int"
        columns["prev_max_interval"] = "string"
        columns["prev_value"] = "bigint"
        return columns

    @staticmethod
    def _split_to_counters() -> bool:
        return False

    def source_table(self):
        raise NotImplementedError()

    @staticmethod
    @abstractmethod
    def _date_add(day: str, intervals: int) -> str:
        raise NotImplementedError()

    @staticmethod
    @abstractmethod
    def _date_diff(date1: str, date2: str) -> str:
        raise NotImplementedError()

    @staticmethod
    @abstractmethod
    def get_max_interval(to_day: str) -> str:
        raise NotImplementedError()

    def life_cycle_days(self) -> int:
        return 180


class DailyMaxCounters(MaxCounters):
    def table_name(self) -> str:
        return "daily_max_counters"

    def table_folder(self) -> str:
        return "daily-max-counters"

    def partition_name(self) -> Optional[str]:
        return "day"

    def _max_not_seen_intervals(self) -> int:
        return 180

    def source_table(self):
        return "daily_counters"

    @staticmethod
    def _date_add(day: str, intervals: int) -> str:
        return f"CAST(DATE_ADD('day', {intervals}, DATE('{day}')) AS VARCHAR)"

    @staticmethod
    def _date_diff(date1: str, date2: str) -> str:
        return f"DATE_DIFF('day', DATE({date1}), DATE({date2}))"

    @staticmethod
    def get_max_interval(to_day: str) -> str:
        return get_yesterday() if to_day is None else to_day


class WeeklyMaxCounters(MaxCounters):
    def table_name(self) -> str:
        return "weekly_max_counters"

    def table_folder(self) -> str:
        return "weekly-max-counters"

    def partition_name(self) -> Optional[str]:
        return "week"

    def _max_not_seen_intervals(self) -> int:
        return 50

    def source_table(self):
        return "weekly_counters"

    @staticmethod
    def _date_add(day: str, intervals: int) -> str:
        return f"CAST(DATE_ADD('week', {intervals}, DATE('{day}')) AS VARCHAR)"

    @staticmethod
    def _date_diff(date1: str, date2: str) -> str:
        return f"DATE_DIFF('week', DATE({date1}), DATE({date2}))"

    @staticmethod
    def get_max_interval(to_day: str) -> str:
        return get_last_finished_week(to_day)


class MonthlyMaxCounters(MaxCounters):
    def table_name(self) -> str:
        return "monthly_max_counters"

    def table_folder(self) -> str:
        return "monthly-max-counters"

    def partition_name(self) -> Optional[str]:
        return "month"

    def partition_value(self, day: str) -> str:
        return day[:7]

    def _max_not_seen_intervals(self) -> int:
        return 18

    def source_table(self):
        return "monthly_counters"

    @staticmethod
    def _date_diff(date1: str, date2: str) -> str:
        return f"DATE_DIFF('month', DATE({date1}||'-01'), DATE({date2}||'-01'))"

    @staticmethod
    def _date_add(month: str, intervals: int) -> str:
        return f"SUBSTRING(CAST(DATE_ADD('month', {intervals}, DATE('{month}-01')) AS VARCHAR), 1, 7)"

    @staticmethod
    def get_max_interval(to_day: str) -> str:
        return get_last_finished_month(to_day)


def _get_max_existing_data(
    creator: MaxCounters, dataset: str, counter_id: int, interval: str, session: Session
) -> Dict[str, Dict[int, bool]]:
    sql = f"""WITH source AS (
SELECT DISTINCT dataset, counter_id 
FROM {creator.database_name()}.{creator.source_table()} WHERE {creator.partition_name()} = '{interval}' 
{f"AND dataset = '{dataset}'" if dataset else ""}
{f"AND counter_id = {counter_id}" if counter_id else ""}),
max_data AS (
SELECT DISTINCT dataset, counter_id 
FROM {creator.full_table_name()} WHERE {creator.partition_name()} = '{interval}'
{f"AND dataset = '{dataset}'" if dataset else ""}
{f"AND counter_id = {counter_id}" if counter_id else ""})
SELECT source.*, max_data.counter_id IS NOT NULL AS has_max
FROM source LEFT OUTER JOIN max_data ON source.dataset = max_data.dataset AND source.counter_id = max_data.counter_id
"""
    query_result = get_query_results(sql, session=session)
    result = {}
    for row in query_result["ResultSet"]["Rows"][1:]:
        dataset = row["Data"][0]["VarCharValue"]
        counter_id = int(row["Data"][1]["VarCharValue"])
        has_max = row["Data"][2]["VarCharValue"] == "true"
        if dataset not in result:
            result[dataset] = {}
        result[dataset][counter_id] = has_max
    return result


def _get_max_table_by_interval_type(interval_type: str) -> str:
    if interval_type == "day":
        return "daily_max_counters"
    elif interval_type == "week":
        return "weekly_max_counters"
    elif interval_type == "month":
        return f"monthly_max_counters"
    else:
        raise Exception(f"Unknown interval type: {interval_type}")


def detect_max_records(
    dataset_name: str = None,
    interval_type: str = None,
    override: bool = False,
    counter_id: int = None,
    from_day: str = None,
    to_day: str = None,
):
    start_time = datetime.now()
    session = get_session()
    errors = 0
    success = 0
    exists = 0
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=get_max_workers(), thread_name_prefix="max_"
    ) as thread_pool:
        futures = []
        for cur_interval_type in (
            [interval_type] if interval_type else ["day", "week", "month"]
        ):
            # noinspection PyTypeChecker
            creator: MaxCounters = get_table_creator(
                _get_max_table_by_interval_type(cur_interval_type)
            )
            interval = creator.get_max_interval(to_day)
            existing_data = _get_max_existing_data(
                creator, dataset_name, counter_id, interval, session
            )
            for d_name in (
                [dataset_name] if dataset_name else get_datasets(session).keys()
            ):
                cur_dataset = get_dataset(name=d_name, session=session)
                for c_id in (
                    get_dataset(d_name, session).counters
                    if counter_id is None
                    else [counter_id]
                ):
                    if cur_dataset.counters[c_id].max_record:
                        log_key = f"dataset: {d_name}, {creator.partition_name()}: {interval}, Counter: {c_id}"
                        if d_name in existing_data and c_id in existing_data[d_name]:
                            if existing_data[d_name][c_id] and not override:
                                if logging.getLogger().isEnabledFor(logging.DEBUG):
                                    logging.debug(
                                        f"Max values data exists for {log_key}"
                                    )
                                exists += 1
                            else:
                                thread_pool.submit(
                                    _run_max_task,
                                    log_key,
                                    cur_interval_type,
                                    interval,
                                    d_name,
                                    c_id,
                                    from_day,
                                )
                        else:
                            logging.info(f"Source data not found for {log_key}")
        for f in concurrent.futures.as_completed(futures):
            if f.result():
                success += 1
            else:
                errors += 1
    elapsed_time = datetime.now() - start_time
    result = {
        "operation": "max",
        "status": "UP",
        "success": success,
        "exists": exists,
        "duration": str(elapsed_time),
    }
    logging.info(result)
    if errors > 0:
        result["errors"] = errors
    return result


def _run_max_task(
    log_key: str,
    interval_type: str,
    interval: str,
    dataset: str,
    counter_id: int,
    from_day: str,
) -> bool:
    try:
        logging.info(f"detecting max values for {log_key}")
        creator = get_table_creator(_get_max_table_by_interval_type(interval_type))
        creator.create_day(
            dataset,
            interval,
            counter_id=counter_id,
            session=get_session(),
            from_day=from_day,
        )
        return True
    except Exception as e:
        logging.exception(
            f"Error collecting data for {log_key}. Error message: {str(e)}"
        )
        return False
