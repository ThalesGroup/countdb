import concurrent.futures
import logging
from datetime import date, datetime
from enum import Enum
from typing import Dict, Optional, Set

from boto3 import Session

from datasets import get_datasets, get_dataset
from s3_utils import folder_last_modified
from s3_utils import get_root_folder
from table_creator import TableCreator
from table_creator import get_table_creator
from utils import (
    get_last_finished_week,
    get_last_finished_month,
    get_session,
    _MAX_WORKERS,
)

_MAX_RSD = 0.3
_MIN_DISTANCE_FROM_RSD = {"day": 0.9, "week": 0.4, "month": 0.4}
_MIN_TREND_SLOPE = 0.1
_DEFAULT_MIN_AVG = 10.0

_MIN_DATA_POINTS = {"day": 30, "week": 20, "month": 10}


class DetectionMethod(Enum):
    Peak = (0,)
    Trend = (1,)
    Pattern = (2,)


def detect_highlights(
    dataset_name: str = None,
    interval_type: str = None,
    from_day: str = None,
    to_day: str = None,
    method: str = None,
    override: bool = False,
):
    """
    Detects highlights for a specified dataset, interval type, and method within a given date range.

    Args:
        dataset_name (str, optional): The name of the dataset to detect highlights for. If None, detects for all
          datasets.
        interval_type (str, optional): The type of interval for detection ('day', 'week', 'month'). If None,
          detects for all intervals.
        from_day (str, optional): The start date for the detection in the format 'YYYY-MM-DD'. If None, uses
          default date range.
        to_day (str, optional): The end date for the detection in the format 'YYYY-MM-DD'. If None, uses default
          date range.
        method (str, optional): The detection method to use ('Peak', 'Trend', 'Pattern'). If None, uses all methods.
        override (bool, optional): If True, overrides existing data. Defaults to False.

    Returns:
        Dict: A dictionary containing the result of the detection operation, including the number of successful detections,
              the number of existing data points, the duration of the operation, and any errors encountered.

    Raises:
        Exception: If an error occurs during the detection process.
    """
    start_time = datetime.now()
    session = get_session()
    errors = 0
    success = 0
    existing = 0
    existing_data = (
        {} if override else _get_existing_data(dataset_name, interval_type, session)
    )
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=_MAX_WORKERS, thread_name_prefix="max_"
    ) as thread_pool:
        futures = []
        for dataset in [dataset_name] if dataset_name else get_datasets(session).keys():
            get_dataset(dataset)
            for d_method in (
                [DetectionMethod[method]]
                if method
                else [
                    DetectionMethod.Trend,
                    DetectionMethod.Peak,
                    DetectionMethod.Pattern,
                ]
            ):
                for interval in (
                    [interval_type] if interval_type else ["day", "week", "month"]
                ):
                    if from_day and to_day:
                        thread_pool.submit(
                            _run_highlight_task,
                            dataset,
                            interval,
                            d_method.name,
                            from_day,
                            to_day,
                        )
                    else:
                        if (
                            dataset_name in existing_data
                            and interval in existing_data[dataset_name]
                            and d_method.name in existing_data[dataset_name][interval]
                        ):
                            logging.info(
                                f"Highlights data already exists. Dataset {dataset_name}, "
                                f"Interval: {interval}, Method: {d_method}"
                            )
                            existing += 1
                        else:
                            thread_pool.submit(
                                _run_highlight_task,
                                dataset,
                                interval,
                                d_method.name,
                                None,
                                None,
                            )
        for f in concurrent.futures.as_completed(futures):
            if f.result():
                success += 1
            else:
                errors += 1
    elapsed_time = datetime.now() - start_time
    result = {
        "operation": "detect",
        "status": "UP",
        "success": success,
        "exists": existing,
        "duration": str(elapsed_time),
    }
    if errors > 0:
        result["errors"] = errors
    logging.info(result)
    return result


def _get_existing_data(
    dataset_name: str = None, interval_type: str = None, session: Session = None
) -> Dict[str, Dict[str, Set[str]]]:
    prefix = f"{get_root_folder()}/tables/highlights/"

    if dataset_name:
        prefix += f"dataset={dataset_name}/"
    if interval_type:
        prefix += f"interval_type={interval_type}/"
    result = {}
    folders = folder_last_modified(prefix, session=session)
    for folder in folders:
        data = folder.split("/")[:-1]
        if dataset_name:
            current_dataset = dataset_name
        else:
            current_dataset = data[0].split("=")[1]
            del data[0]
        if interval_type:
            interval = interval_type
        else:
            interval = data[0].split("=")[1]
            del data[0]
        last_modified = folders[folder]
        if (
            interval == "day"
            and last_modified == str(date.today())
            or interval == "week"
            and last_modified >= get_last_finished_week()
            or interval == "month"
            and last_modified >= get_last_finished_month()
        ):
            method = data[0].split("=")[1]
            if current_dataset not in result:
                result[current_dataset] = {}
            if interval not in result[current_dataset]:
                result[current_dataset][interval] = set()
            result[current_dataset][interval].add(method)
    return result


def _run_highlight_task(
    dataset: str,
    interval: str,
    method: str,
    from_day: Optional[str],
    to_day: Optional[str],
) -> bool:
    try:
        logging.info(
            f"detecting highlights for Dataset {dataset}, Interval: {interval}, Method: {method}"
        )
        get_table_creator("highlights").create_day(
            dataset,
            interval,
            session=get_session(),
            from_day=from_day,
            to_day=to_day,
            method=method,
        )
        return True
    except Exception as e:
        logging.exception(
            f"Error collecting data for Dataset {dataset}, Interval: {interval}, Method: {method}. "
            f"Error message: {str(e)}"
        )
        return False


def _get_counters_table_by_interval_type(interval_type: str) -> str:
    if interval_type == "day":
        return "daily_counters"
    elif interval_type == "week":
        return "weekly_counters"
    elif interval_type == "month":
        return "monthly_counters"
    else:
        raise Exception(f"Unknown interval type: {interval_type}")


def _get_filter_by_interval_type(interval_type: str, from_day: str, to_day: str) -> str:
    if interval_type == "day":
        if from_day is None:
            return "DATE(day) >= DATE_ADD('day', -90, CURRENT_DATE)"
        else:
            return f"day BETWEEN '{from_day}' AND '{to_day}'"
    elif interval_type == "week":
        if from_day is None:
            return "DATE(week) >= DATE_ADD('week', -25, CURRENT_DATE)"
        else:
            return f"week BETWEEN '{from_day}' AND '{to_day}'"
    elif interval_type == "month":
        if from_day is None:
            return "month >= SUBSTRING(CAST(CURRENT_DATE - INTERVAL '12' MONTH AS VARCHAR), 1, 7)"
        else:
            return f"month BETWEEN '{from_day[:7]}' AND '{to_day[:7]}'"
    else:
        raise Exception(f"Unknown interval type: {interval_type}")


def _get_counters_data_sql(
    database_name: str,
    dataset: str,
    interval_type: str,
    from_day: str = None,
    to_day: str = None,
) -> str:
    return f"""SELECT counter_id,  
  CASE 
    WHEN int_key1 IS NOT NULL AND int_key2 IS NOT NULL THEN ARRAY[int_key1, int_key2]
    WHEN int_key1 IS NOT NULL AND int_key2 IS NULL THEN ARRAY[int_key1]
    ELSE ARRAY[]
  END AS int_key,
  CASE
    WHEN str_key1 IS NOT NULL AND str_key2 IS NOT NULL THEN ARRAY[str_key1, str_key2]
    WHEN str_key1 IS NOT NULL AND str_key2 IS NULL THEN ARRAY[str_key1]
   ELSE ARRAY[]
  END AS str_key, 
  {interval_type} AS interval, value, min_avg 
FROM {database_name}.{_get_counters_table_by_interval_type(interval_type)} 
     INNER JOIN {database_name}.counters_metadata USING (dataset, counter_id)
WHERE dataset = '{dataset}'
      AND {_get_filter_by_interval_type(interval_type, from_day, to_day)}"""


def _detection_sql_by_interval_type(
    database_name: str,
    dataset: str,
    method: DetectionMethod,
    interval_type: str,
    from_day: str = None,
    to_day: str = None,
):
    counters_sql = _get_counters_data_sql(
        database_name, dataset, interval_type, from_day, to_day
    )
    return get_detection_method_sql(method, interval_type, counters_sql)


def get_detection_method_sql(
    detection_method: DetectionMethod, interval_type: str, inner_sql: str
) -> str:
    if detection_method == DetectionMethod.Peak:
        return _get_peak_detection_sql(inner_sql, interval_type)
    elif detection_method == DetectionMethod.Trend:
        return _get_trend_detection_sql(inner_sql, interval_type)
    elif detection_method == DetectionMethod.Pattern:
        return _get_pattern_detection_sql(inner_sql)
    else:
        raise ValueError(f"Unknown detection method: {detection_method}")


def _get_peak_detection_sql(inner_sql: str, interval_type: str) -> str:
    return f"""
WITH data AS ({inner_sql.strip('\n ')}),
stats AS (
  SELECT
    data.*,
    AVG(value) OVER (PARTITION BY counter_id, int_key, str_key) AS avg_value,
    STDDEV(value) OVER (PARTITION BY counter_id, int_key, str_key) AS stddev_value,
    APPROX_PERCENTILE(value, 0.75) OVER (PARTITION BY counter_id, int_key, str_key) AS q3,
    APPROX_PERCENTILE(value, 0.75) OVER (PARTITION BY counter_id, int_key, str_key) -
      APPROX_PERCENTILE(value, 0.25) OVER (PARTITION BY counter_id, int_key, str_key) AS iqr
    FROM data),
anomalies AS (
  SELECT detection, counter_id, int_key, str_key, ARRAY_AGG(interval ORDER BY interval) AS anomaly_intervals
  FROM stats CROSS JOIN LATERAL
    (SELECT CASE
        WHEN stddev_value / avg_value <= {_MAX_RSD}
             AND ABS(1 - value / avg_value) >= {_MIN_DISTANCE_FROM_RSD[interval_type]} THEN 'RSD'
        WHEN value > avg_value + 3 * stddev_value THEN 'EmpiricalRule'
        WHEN value > q3 + 1.5 * iqr THEN 'IQR'
       ELSE NULL
      END AS detection)
  WHERE avg_value >= COALESCE(min_avg, {_DEFAULT_MIN_AVG})
        AND detection IS NOT NULL
  GROUP BY detection, counter_id, int_key, str_key)
SELECT '{DetectionMethod.Peak.name}' AS method,
  detection AS sub_method,
  counter_id,
  CASE WHEN CARDINALITY(int_key) = 0 THEN NULL ELSE int_key END AS int_key,
  CASE WHEN CARDINALITY(str_key) = 0 THEN NULL ELSE str_key END AS str_key,
  ARBITRARY(anomaly_intervals) AS anomalies,
  ARRAY_AGG(interval ORDER BY interval) AS intervals,
  ARRAY_AGG(value ORDER BY interval) AS vals,
  counter_id AS cnt_id
FROM data INNER JOIN anomalies USING (counter_id, int_key, str_key)
GROUP BY detection, counter_id, int_key, str_key
HAVING COUNT() >= {_MIN_DATA_POINTS[interval_type]}"""


def _get_trend_detection_sql(inner_sql: str, interval_type: str) -> str:
    return f"""
WITH data AS (
  {inner_sql}),
change AS (
  SELECT data.*,
         RANK() OVER (PARTITION BY counter_id, int_key, str_key ORDER BY interval) AS interval_rank,
         1.0 * value / MIN_BY(value, interval) OVER (PARTITION BY counter_id, int_key, str_key
                                                     ORDER BY interval) AS change
  FROM data)
SELECT '{DetectionMethod.Trend.name}' AS method,
  counter_id,
  CASE WHEN CARDINALITY(int_key) = 0 THEN NULL ELSE int_key END AS int_key,
  CASE WHEN CARDINALITY(str_key) = 0 THEN NULL ELSE str_key END AS str_key,
  ARRAY_AGG(interval ORDER BY interval) AS intervals,
  ARRAY_AGG(value ORDER BY interval) AS vals,
  counter_id AS cnt_id
FROM change
GROUP BY counter_id, int_key, str_key
HAVING COUNT() >= {_MIN_DATA_POINTS[interval_type]}
       AND AVG(value) >= COALESCE(ARBITRARY(min_avg), {_DEFAULT_MIN_AVG})
       AND REGR_SLOPE(change, interval_rank)  >= {_MIN_TREND_SLOPE}
ORDER BY ABS(REGR_SLOPE(change, interval_rank)) DESC
LIMIT 20
"""


def _get_pattern_detection_sql(inner_sql: str) -> str:
    pattern_str = "MIN_AVG START UP{2, } FINAL_UP$"
    return f"""
WITH data AS (
  {inner_sql}),
stats AS (
    SELECT counter_id, int_key, str_key, AVG(value) AS avg_value
    FROM data
    GROUP BY counter_id, int_key, str_key
    HAVING AVG(value) >= {_DEFAULT_MIN_AVG}),
matches AS (
  SELECT counter_id, int_key, str_key, top_interval
  FROM (data INNER JOIN stats USING (counter_id, int_key, str_key))
  MATCH_RECOGNIZE(
     PARTITION BY counter_id, int_key, str_key
     ORDER BY interval
     MEASURES
         LAST(FINAL_UP.interval) AS top_interval
     AFTER MATCH SKIP PAST LAST ROW
     PATTERN ({pattern_str})
     DEFINE
         UP AS value >= PREV(value),
         FINAL_UP AS value >= PREV(value) AND value >= START.value * 3 AND value >= avg_value * 1.75)),
matches_agg AS (
  SELECT counter_id, int_key, str_key, ARRAY_AGG(top_interval ORDER BY top_interval) AS anomalies
  FROM matches
  GROUP BY counter_id, int_key, str_key)
SELECT '{DetectionMethod.Pattern.name}' AS method, 'RecentIncrease' AS sub_method,
  counter_id, int_key, str_key,
  ARRAY_AGG(interval ORDER BY interval) AS intervals,
  ARRAY_AGG(value ORDER BY interval) AS vals,
  ARBITRARY(anomalies) AS anomalies
FROM data INNER JOIN matches_agg USING (counter_id, int_key, str_key)
GROUP BY counter_id, int_key, str_key
"""


class HighlightsCreator(TableCreator):
    def table_name(self) -> str:
        return "highlights"

    def table_folder(self) -> str:
        return "highlights"

    def get_sql(
        self,
        dataset: str,
        interval_type: str,
        counter_id: int = None,
        session: Session = None,
        **kwargs,
    ) -> str:
        detection_method = DetectionMethod[kwargs["method"]]
        return _detection_sql_by_interval_type(
            self.database_name(),
            dataset,
            detection_method,
            interval_type,
            kwargs.get("from_day"),
            kwargs.get("to_day"),
        )

    def partition_name(self) -> Optional[str]:
        return "interval_type"

    @staticmethod
    def external_table_columns() -> Dict[str, str]:
        return {
            "method": "string",
            "sub_method": "string",
            "counter_id": "bigint",
            "int_key": "ARRAY<bigint>",
            "str_key": "ARRAY<string>",
            "anomalies": "ARRAY<string>",
            "intervals": "ARRAY<string>",
            "vals": "ARRAY<bigint>",
        }

    @staticmethod
    def _split_to_counters() -> bool:
        return False

    def _partition_path(self, dataset: str, day: str, **kwargs) -> str:
        path = super()._partition_path(dataset, day, **kwargs)
        return f"{path}/method={kwargs['method']}"

    def life_cycle_days(self) -> int:
        return 60
