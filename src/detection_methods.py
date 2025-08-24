import concurrent.futures
import logging
from datetime import date, datetime
from enum import Enum
from typing import Dict, Optional, Set, List, Generator, Tuple

from boto3 import Session

from datasets import get_datasets, get_dataset, _CounterDataset, _Counter
from s3_utils import list_s3_folder_keys, upload_content_to_s3, s3_folder_tree
from s3_utils import get_root_folder
from table_creator import TableCreator
from table_creator import get_table_creator
from utils import (
    get_last_finished_week,
    get_last_finished_month,
    get_session,
    get_max_workers,
)

_DETECT_TIMEOUT_SECONDS = 900
_MAX_RSD = 0.3
_MIN_DISTANCE_FROM_RSD = {"day": 0.9, "week": 0.4, "month": 0.4}
_MIN_TREND_SLOPE = 0.1
_DEFAULT_IQR = 1.5

_MIN_DATA_POINTS = {"day": 30, "week": 20, "month": 10}
_DATA_POINTS = {"day": 90, "week": 25, "month": 12}


class DetectionMethod(Enum):
    Peak = (0,)
    Trend = (1,)
    Pattern = (2,)


def detect_highlights(
    dataset_name: str = None,
    interval_type: str = None,
    method: str = None,
    counter_id: int = None,
    from_day: str = None,
    to_day: str = None,
    override: bool = False,
):
    """
    Detects highlights for a specified dataset, interval type, and method within a given date range.

    Args:
        dataset_name (str, optional): The name of the dataset to detect highlights for. If None, detects for all
          datasets.
        interval_type (str, optional): The type of interval for detection ('day', 'week', 'month'). If None,
          detects for all intervals.
        counter_id (int, optional): The ID of the counter to detect highlights for. If None, detects for all counters.
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
    existing, longest_duration = 0, 0
    existing_data = (
        {}
        if override or from_day or to_day
        else _get_existing_data(
            dataset_name, interval_type, method, counter_id, session
        )
    )
    shutdown_called = False
    results: List[Dict] = []
    workers = get_max_workers()
    logging.info(f"Detecting highlights. Workers: {workers}")
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=workers, thread_name_prefix="max_"
    ) as thread_pool:
        futures = []
        for dataset, d_method, interval, cnt_id in _get_detection_tasks(
            session, dataset_name, interval_type, method, counter_id
        ):
            if (
                existing_data.get(dataset, {})
                .get(interval, {})
                .get(d_method.name, {})
                .get(int(cnt_id), False)
            ):
                if logging.getLogger().isEnabledFor(logging.DEBUG):
                    logging.debug(
                        f"Highlights data already exists. Dataset {dataset}, "
                        f"Interval: {interval}, Method: {d_method}, Counter ID: {cnt_id}"
                    )
                existing += 1
            else:
                future = thread_pool.submit(
                    _run_highlight_task,
                    dataset,
                    interval,
                    d_method.name,
                    cnt_id,
                    from_day,
                    to_day,
                )
                futures.append(future)
        logging.info(f"Submitted: {len(futures)} tasks. Found {existing} existing data")
        for f in concurrent.futures.as_completed(futures):
            results.append(f.result())
            longest_duration = max(
                longest_duration, f.result().get("duration-seconds", 0)
            )
            if _should_stop(start_time, _DETECT_TIMEOUT_SECONDS, longest_duration):
                thread_pool.shutdown(wait=False, cancel_futures=True)
                shutdown_called = True
                break
    if shutdown_called:
        for f in concurrent.futures.as_completed(
            [f for f in futures if not f.cancelled()]
        ):
            results.append(f.result())
    elapsed_time = datetime.now() - start_time
    result = {
        "operation": "detect",
        "success": sum(1 for r in results if r["success"]),
        "errors": sum(1 for r in results if not r["success"]),
        "exists": existing,
        "no_data": sum(1 for r in results if r["success"] and not r["has_data"]),
        "cancelled": sum(1 for f in futures if f.cancelled()),
        "duration": str(elapsed_time),
    }
    logging.info(result)
    return result


def _get_detection_tasks(
    session: Session,
    dataset_name: str = None,
    interval_type: str = None,
    method: str = None,
    counter_id: int = None,
) -> Generator[Tuple[str, DetectionMethod, str, int], None, None]:
    for dataset in [dataset_name] if dataset_name else get_datasets(session).keys():
        d = get_dataset(dataset, session)
        for d_method in _get_detection_methods(method, d):
            for interval in (
                [interval_type] if interval_type else ["day", "week", "month"]
            ):
                for cnt_id in (
                    [counter_id] if counter_id else [cnt_id for cnt_id in d.counters]
                ):
                    yield dataset, d_method, interval, cnt_id


def _should_stop(
    start_time: datetime, timeout_seconds: int, longest_duration: int
) -> bool:
    elapsed_time = (datetime.now() - start_time).seconds
    if elapsed_time >= timeout_seconds:
        logging.info(
            f"Timeout of {timeout_seconds} seconds occurred. Elapsed time: {elapsed_time} seconds"
        )
        return True
    if longest_duration * 1.2 >= timeout_seconds - elapsed_time:
        logging.info(
            f"Canceling future tasks. Longest duration: {longest_duration} seconds. "
            f"Remaining time until timeout: {timeout_seconds - elapsed_time} seconds"
        )
        return True
    return False


def _get_detection_methods(
    method: Optional[str], dataset: _CounterDataset
) -> Set[DetectionMethod]:
    if method:
        return {DetectionMethod[method]}
    elif len(dataset.detection_methods) > 0:
        return {DetectionMethod[m] for m in dataset.detection_methods}
    else:
        # By default, use Peak detection method
        return {
            DetectionMethod.Peak,
        }


def _remove_key_prefix(d: dict):
    if not isinstance(d, dict):
        return d
    new_d = {}
    for k, v in d.items():
        # Remove prefix before '=' if present
        new_key = k.split("=", 1)[1] if "=" in k else k
        # Recurse into sub-dictionaries
        new_d[new_key] = _remove_key_prefix(v)
    return new_d


def _build_existing_data(result: dict):
    result = _remove_key_prefix(result)
    for dataset in result:
        for interval in result[dataset]:
            for method in result[dataset][interval]:
                counters = {}
                for counter_id in result[dataset][interval][method]:
                    last_modified = result[dataset][interval][method][counter_id]
                    if (
                        interval == "day"
                        and last_modified == str(date.today())
                        or interval == "week"
                        and last_modified >= get_last_finished_week()
                        or interval == "month"
                        and last_modified >= get_last_finished_month()
                    ):
                        counters[int(counter_id)] = True
                result[dataset][interval][method] = counters
    return result


def _get_existing_data(
    dataset_name: str = None,
    interval_type: str = None,
    method: str = None,
    counter_id: int = None,
    session: Session = None,
) -> Dict[str, Dict[str, Set[str]]]:
    root = f"{get_root_folder()}/tables/highlights/"
    prefix = ""
    if dataset_name:
        prefix += f"dataset={dataset_name}/"
        if interval_type:
            prefix += f"interval_type={interval_type}/"
            if method:
                prefix += f"method={method}/"
                if counter_id:
                    prefix += f"cnt_id={counter_id}/"
    folders = s3_folder_tree(root, prefix, session)
    return _build_existing_data(folders)


def _run_highlight_task(
    dataset: str,
    interval: str,
    method: str,
    counter_id: int,
    from_day: Optional[str],
    to_day: Optional[str],
) -> Dict[str, any]:
    try:
        start_time = datetime.now()
        session = get_session()
        counter = get_dataset(dataset, session).counters[counter_id]
        logging.info(
            f"detecting highlights for Dataset {dataset}, Interval: {interval}, Method: {method}. Counter ID: {counter_id}. Counter: {counter.name} Timeout: {counter.timeout} seconds"
        )
        result = get_table_creator("highlights").create_day(
            dataset,
            day=interval,
            interval=interval,
            method=method,
            interval_type=interval,
            session=session,
            counter_id=counter_id,
            from_day=from_day,
            to_day=to_day,
            timeout=counter.timeout,
        )
        has_data = (
            len(list_s3_folder_keys(result["partition_path"], session=session)) > 0
        )
        if not has_data:
            upload_content_to_s3(
                f"{result['partition_path']}/_SUCCESS_NO_DATA", "", session=session
            )
        return {
            "success": True,
            "has_data": has_data,
            "duration-seconds": (datetime.now() - start_time).seconds,
        }
    except Exception as e:
        logging.exception(
            f"Error detecting data for Dataset {dataset}, Interval: {interval}, Method: {method}. Counter ID: {counter_id}. "
            f"Error message: {str(e)}"
        )
        return {
            "success": False,
        }


def _get_counters_table_by_interval_type(interval_type: str) -> str:
    if interval_type == "day":
        return "daily_counters"
    elif interval_type == "week":
        return "weekly_counters"
    elif interval_type == "month":
        return "monthly_counters"
    else:
        raise Exception(f"Unknown interval type: {interval_type}")


def _get_filter_by_interval_type(
    interval_type: str, from_day: Optional[str], to_day: Optional[str]
) -> str:
    d_points = _DATA_POINTS[interval_type]
    if interval_type == "day":
        if from_day is None:
            return f"DATE(day) BETWEEN DATE_ADD('day', -{d_points}, CURRENT_DATE) AND DATE_ADD('day', -1, CURRENT_DATE)"
        else:
            return f"day BETWEEN '{from_day}' AND '{to_day}'"
    elif interval_type == "week":
        if from_day is None:
            _LAST_WEEK_EXP = (
                "DATE_ADD('day', -DAY_OF_WEEK(CURRENT_DATE) - 6, CURRENT_DATE)"
            )
            return f"DATE(week) BETWEEN DATE_ADD('week', -{d_points}, {_LAST_WEEK_EXP}) AND {_LAST_WEEK_EXP}"
        else:
            return f"week BETWEEN '{from_day}' AND '{to_day}'"
    elif interval_type == "month":
        if from_day is None:
            return f"month >= SUBSTRING(CAST(CURRENT_DATE - INTERVAL '{d_points}' MONTH AS VARCHAR), 1, 7)"
        else:
            return f"month BETWEEN '{from_day[:7]}' AND '{to_day[:7]}'"
    else:
        raise Exception(f"Unknown interval type: {interval_type}")


def _get_counters_data_sql(
    database_name: str,
    dataset: str,
    interval_type: str,
    counter_id: int,
    from_day: str = None,
    to_day: str = None,
    add_interval_rank: bool = False,
    add_count: bool = False,
    add_avg: bool = False,
) -> str:
    int_rank = f"ROW_NUMBER() OVER (PARTITION BY int_key1, int_key2, str_key1, str_key2 ORDER BY {interval_type}) AS interval_rank"
    cnt_exp = (
        "COUNT() OVER (PARTITION BY int_key1, int_key2, str_key1, str_key2) AS cnt"
    )
    avg_exp = "AVG(value) OVER (PARTITION BY int_key1, int_key2, str_key1, str_key2) AS avg_value"
    result = f"""SELECT int_key1, int_key2, str_key1, str_key2,{interval_type} AS interval, value  
{"," + int_rank if add_interval_rank else ""}
{"," + cnt_exp if add_count else ""}
{"," + avg_exp if add_avg else ""}
FROM {database_name}.{_get_counters_table_by_interval_type(interval_type)} 
WHERE dataset = '{dataset}'
      AND {_get_filter_by_interval_type(interval_type, from_day, to_day)}"""
    if counter_id is not None:
        result += f"\nAND counter_id = {counter_id}"
    return result


def _get_detection_sql(
    database_name: str,
    dataset: str,
    detection_method: DetectionMethod,
    interval_type: str,
    counter_id: int,
    from_day: str,
    to_day: str,
    session: Session,
):
    counter = get_dataset(dataset, session).counters[counter_id]
    if detection_method == DetectionMethod.Peak:
        return _get_peak_detection_sql(
            database_name, dataset, interval_type, counter, from_day, to_day
        )
    elif detection_method == DetectionMethod.Trend:
        return _get_trend_detection_sql(
            database_name, dataset, interval_type, counter, from_day, to_day
        )
    elif detection_method == DetectionMethod.Pattern:
        return _get_pattern_detection_sql(
            database_name, dataset, interval_type, counter, from_day, to_day
        )
    else:
        raise ValueError(f"Unknown detection method: {detection_method}")


def _get_final_detection_sql(
    counter_id: int,
    method: DetectionMethod,
    has_sub_method: bool,
    anomalies_exp: str = "",
) -> str:
    return f"""SELECT '{method.name}' AS method,
       {counter_id} AS counter_id,
       {"ARBITRARY(sub_method) AS sub_method," if has_sub_method else ""}
       CASE WHEN d.int_key1 IS NULL THEN NULL
            WHEN d.int_key2 IS NULL THEN ARRAY[d.int_key1]
            ELSE ARRAY[d.int_key1, d.int_key2] END AS int_key,
       CASE WHEN d.str_key1 IS NULL THEN NULL
            WHEN d.str_key2 IS NULL THEN ARRAY[d.str_key1]
            ELSE ARRAY[d.str_key1, d.str_key2] END AS str_key,    
       {anomalies_exp + "AS anomalies," if anomalies_exp else ""}
       ARRAY_SORT(ARRAY_AGG(interval)) AS intervals,
       ARRAY_AGG(value ORDER BY interval) AS vals
FROM data AS d
INNER JOIN anomalies a ON
 (d.int_key1 IS NULL AND a.int_key1 IS NULL OR d.int_key1 = a.int_key1)
 AND (d.int_key2 IS NULL AND a.int_key2 IS NULL OR d.int_key2 = a.int_key2)
 AND (d.str_key1 IS NULL AND a.str_key1 IS NULL OR d.str_key1 = a.str_key1)
 AND (d.str_key2 IS NULL AND a.str_key2 IS NULL OR d.str_key2 = a.str_key2) 
GROUP BY d.int_key1, d.int_key2, d.str_key1, d.str_key2"""


def _get_peak_detection_sql(
    database_name: str,
    dataset: str,
    interval_type: str,
    counter: _Counter,
    from_day: str = None,
    to_day: str = None,
) -> str:
    return f"""WITH data  AS (
  {_get_counters_data_sql(database_name, dataset, interval_type, counter.id, from_day, to_day)}),
stats AS (
    SELECT int_key1, int_key2, str_key1, str_key2,
           AVG(value) AS avg_value,
           STDDEV(value) AS stddev_value,
           MAX(value) AS max_value,
           APPROX_PERCENTILE(value, 0.25) AS q1,
           APPROX_PERCENTILE(value, 0.75) AS q3
    FROM data
    GROUP BY int_key1, int_key2, str_key1, str_key2
    HAVING COUNT() >= {_MIN_DATA_POINTS[interval_type]}
    AND AVG(value) >= {counter.min_avg}
),
anomalies AS (
  SELECT int_key1, int_key2, str_key1, str_key2, threshold,
    CASE
      WHEN threshold = iqr_threshold THEN 'IQR'
      ELSE 'RSD' 
    END AS sub_method
  FROM stats CROSS JOIN LATERAL (
    SELECT q3 + 3 * (q3 - q1) AS iqr_threshold,
      CASE 
        WHEN stddev_value / NULLIF(avg_value, 0) > 0.3 THEN NULL
        ELSE avg_value * 1.9 
      END AS rsd_threshold)
  CROSS JOIN LATERAL (
    SELECT 
      CASE 
        WHEN rsd_threshold IS NULL THEN iqr_threshold
        ELSE LEAST(rsd_threshold, iqr_threshold) 
      END AS threshold
    )
    WHERE max_value >= threshold
)  
{_get_final_detection_sql(counter.id, DetectionMethod.Peak, True, 
                          "FILTER(ARRAY_SORT(ARRAY_AGG(IF(value > a.threshold, interval))), x -> x IS NOT NULL)")}         
"""


def _get_trend_detection_sql(
    database_name: str,
    dataset: str,
    interval_type: str,
    counter: _Counter,
    from_day: str = None,
    to_day: str = None,
) -> str:
    return f"""
WITH data AS (
  {_get_counters_data_sql(database_name, dataset, interval_type, counter.id, from_day, to_day, True, True)}),   
stats AS (
  SELECT int_key1, int_key2, str_key1, str_key2,
    AVG(value) AS avg_value,
    MIN(value) AS min_value,
    MAX(value) AS max_value,
    STDDEV(value) AS stddev_value,
    REGR_SLOPE(value, interval_rank) AS trend_slope,
    CORR(value, interval_rank) AS correlation,
    -- Calculate relative change from first to last value
    (MAX(value) - MIN(value)) / NULLIF(MIN(value), 0) AS relative_change,
    -- Get recent vs early period averages for trend confirmation  
    AVG(CASE WHEN interval_rank > 0.7 * cnt THEN value END) AS recent_avg,  -- Last ~30%
    AVG(CASE WHEN interval_rank <= 0.3 * cnt THEN value END) AS early_avg   -- First ~30%
  FROM data  
  GROUP BY int_key1, int_key2, str_key1, str_key2
  HAVING COUNT() >= {_MIN_DATA_POINTS[interval_type]}
  AND AVG(value) >= {counter.min_avg}),
anomalies AS (
  SELECT 
        int_key1, int_key2, str_key1, str_key2,        
        CASE 
            WHEN trend_strength >= 0.8 THEN 'Strong'
            WHEN trend_strength >= 0.4 THEN 'Moderate'  
            ELSE 'Weak'
        END AS sub_method
    FROM stats CROSS JOIN LATERAL (
        SELECT ABS(trend_slope) * ABS(correlation) AS trend_strength)
    WHERE ABS(trend_slope) >= 0.1  -- Significant slope
        AND ABS(correlation) >= 0.5  -- Good correlation (trend is not just noise)
        AND ABS(relative_change) >= 0.2  -- Meaningful relative change
        -- recent period significantly different from early period
        AND ABS(recent_avg - early_avg) / NULLIF(early_avg, 0) >= 0.15)
{_get_final_detection_sql(counter.id, DetectionMethod.Trend, True)}
"""


def _get_pattern_detection_sql(
    database_name: str,
    dataset: str,
    interval_type: str,
    counter: _Counter,
    from_day: str = None,
    to_day: str = None,
) -> str:
    pattern_str = "START UP{3, } FINAL_UP$"
    return f"""
WITH data AS (
  {_get_counters_data_sql(database_name, dataset, interval_type, counter.id, from_day, to_day, False, True, True)}),
anomalies AS (
  SELECT int_key1, int_key2, str_key1, str_key2, top_interval, 'RecentIncrease' AS sub_method
  FROM (SELECT * FROM data WHERE cnt >= {_MIN_DATA_POINTS[interval_type]} AND avg_value >= {counter.min_avg}) 
  MATCH_RECOGNIZE(
     PARTITION BY int_key1, int_key2, str_key1, str_key2
     ORDER BY interval
     MEASURES
         LAST(FINAL_UP.interval) AS top_interval
     AFTER MATCH SKIP PAST LAST ROW
     PATTERN ({pattern_str})
     DEFINE
         UP AS value >= PREV(value),
         FINAL_UP AS value >= PREV(value) AND value >= START.value * 1.5 AND value >= avg_value * 1.75))
{_get_final_detection_sql(counter.id, DetectionMethod.Pattern, True, "ARRAY[ARBITRARY(top_interval)]")}
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
        return _get_detection_sql(
            self.database_name(),
            dataset,
            detection_method,
            interval_type,
            counter_id,
            kwargs.get("from_day"),
            kwargs.get("to_day"),
            session,
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

    def _partition_path(self, dataset: str, day: str, **kwargs) -> str:
        path = super()._partition_path(dataset, day, **kwargs)
        return f"{path}/method={kwargs['method']}"

    def life_cycle_days(self) -> int:
        return 60
