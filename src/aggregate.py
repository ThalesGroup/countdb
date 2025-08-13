import concurrent.futures
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, Set, List, Iterable, Optional

from boto3 import Session


from counters import BasCountersCreator
from counters_utils import get_existing_data
from datasets import get_datasets, _Counter, get_dataset
from table_creator import get_table_creator
from utils import (
    get_session,
    date_add,
    get_last_day_of_month,
    weeks_range,
    month_range,
    get_last_finished_week,
    get_yesterday,
    get_last_finished_month,
    _MAX_WORKERS,
    days_range,
)


class PeriodCountersCreator(BasCountersCreator, ABC):
    @staticmethod
    def _get_counters_by_aggregation(
        counters: Iterable[_Counter],
    ) -> Dict[str, List[int]]:
        result = {}
        for c in counters:
            if c.aggregate:
                if c.aggregation_method not in result:
                    result[c.aggregation_method] = []
                result[c.aggregation_method].append(c.id)
        return result

    def should_create(
        self, dataset: str, counter_id: int = None, session: Session = None
    ) -> bool:
        if counter_id:
            return True
        for c in get_dataset(dataset, session).counters.values():
            if c.aggregate:
                return True
        logging.info(f"Dataset: {dataset}. No counters to aggregate")
        return False

    def get_sql(
        self,
        dataset: str,
        day: str,
        counter_id: int = None,
        session: Session = None,
        **kwargs,
    ) -> str:
        counters_to_use = (
            [get_dataset(dataset, session).counters[counter_id]]
            if counter_id
            else get_dataset(dataset, session).counters.values()
        )
        counters_by_agg = self._get_counters_by_aggregation(counters_to_use)
        result = ""
        for agg_method in counters_by_agg:
            if len(result) > 0:
                result += "\nUNION ALL\n"
            result += f"""(SELECT counter_id, int_key1, int_key2, str_key1, str_key2, 
  {agg_method}(value) AS value, counter_id AS cnt_id
FROM {self.database_name()}.daily_counters
WHERE dataset = '{dataset}'
    AND {self._get_period_filter(day)}
    AND counter_id IN ({str(counters_by_agg[agg_method])[1:-1]})
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1, 2, 3, 4, 5)"""
        return result

    @staticmethod
    @abstractmethod
    def _get_period_expression() -> str:
        raise NotImplementedError()

    @staticmethod
    @abstractmethod
    def _get_period_filter(day: str) -> str:
        raise NotImplementedError()


class WeeklyCountersCreator(PeriodCountersCreator):
    @staticmethod
    def _get_period_expression() -> str:
        return f"DATE_ADD('day', -DAY_OF_WEEK(DATE(day)) + 1, DATE(day))"

    @staticmethod
    def _get_period_filter(day: str) -> str:
        return f"day BETWEEN '{day}' AND CAST(DATE_ADD('day', 6, DATE('{day}')) AS VARCHAR)"

    def table_name(self):
        return "weekly_counters"

    def table_folder(self):
        return "weekly-counters"

    def partition_name(self) -> Optional[str]:
        return "week"

    def life_cycle_days(self) -> int:
        return 365


class MonthlyCountersCreator(PeriodCountersCreator):
    @staticmethod
    def _get_period_expression() -> str:
        return f"SUBSTRING(day, 1, 7)"

    @staticmethod
    def _get_period_filter(month: str) -> str:
        return f"day LIKE '{month}-%'"

    def table_name(self):
        return "monthly_counters"

    def table_folder(self):
        return "monthly-counters"

    def partition_name(self) -> Optional[str]:
        return "month"

    def partition_value(self, day: str) -> str:
        return day[:7]

    def life_cycle_days(self) -> int:
        return -1


def aggregate(
    from_day: str,
    to_day: str,
    override: bool = False,
    dataset_name: str = None,
    interval_type: str = None,
    counter_ids: List[int] = None,
) -> Dict:
    """
    Aggregates counters for a specified date range, dataset, and interval type.

    Args:
        from_day (str): The start date for the aggregation in the format 'YYYY-MM-DD'.
        to_day (str): The end date for the aggregation in the format 'YYYY-MM-DD'.
        override (bool, optional): If True, overrides existing data. Defaults to False.
        dataset_name (str, optional): The name of the dataset to aggregate counters for. If None, aggregates for
          all datasets.
        interval_type (str, optional): The type of interval for aggregation ('week' or 'month'). If None,
          aggregates for both.
        counter_ids (int, optional): The ID of the counter to aggregate. If None, aggregates all counters.

    Returns:
        Dict: A dictionary containing the result of the aggregation operation, including the number of successful
          aggregations, the number of existing data points, the number of missing days, the duration of the
          operation, and any errors encountered.
    """
    start_time = datetime.now()
    session = get_session()
    exists = 0
    missing_days = 0
    success = 0
    errors = 0
    if from_day is not None and to_day is not None:
        if from_day > to_day:
            raise Exception("Start day must be before end day")
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=_MAX_WORKERS, thread_name_prefix="aggregate_"
    ) as thread_pool:
        for interval_type in [interval_type] if interval_type else ["week", "month"]:
            intervals = list(
                _get_aggregate_interval_range(interval_type, from_day, to_day)
            )
            table_name = _get_table_name_by_interval(interval_type)
            futures = []
            if len(intervals) > 0:
                interval_existing_data = (
                    {}
                    if override
                    else _get_existing_data_for_intervals(
                        table_name,
                        interval_type,
                        intervals,
                        dataset_name,
                        counter_ids,
                        session,
                    )
                )
                daily_existing_data = _get_existing_data_for_intervals(
                    "daily_counters",
                    interval_type,
                    intervals,
                    dataset_name,
                    counter_ids=counter_ids,
                    session=session,
                )
                for dataset in (
                    [dataset_name] if dataset_name else get_datasets(session).keys()
                ):
                    for interval in intervals:
                        log_str = (
                            f"Aggregate. Dataset: {dataset}. Interval type: {interval_type}. "
                            f"Interval: {interval}. Counter ID: {str(counter_ids) if counter_ids else 'ALL'}"
                        )
                        if (
                            dataset in interval_existing_data
                            and interval in interval_existing_data[dataset]
                        ):
                            logging.info(f"Data exists for for {log_str}")
                            exists += 1
                        elif not _daily_data_exists(
                            daily_existing_data, dataset, interval_type, interval
                        ):
                            logging.warning(f"Missing  daily data for {log_str}")
                            missing_days += 1
                        else:
                            futures.append(
                                thread_pool.submit(
                                    _run_agg_task,
                                    dataset,
                                    log_str,
                                    table_name,
                                    interval,
                                    counter_ids,
                                )
                            )
            for f in concurrent.futures.as_completed(futures):
                if f.result():
                    success += 1
                else:
                    errors += 1
        elapsed_time = datetime.now() - start_time
        result = {
            "operation": "aggregate",
            "success": success,
            "missing_days": missing_days,
            "exists": exists,
            "duration": str(elapsed_time),
        }
        if errors > 0:
            result["errors"] = errors
        return result


def _run_agg_task(
    dataset: str, log_str: str, table_name: str, interval: str, counter_ids: List[int]
) -> bool:
    session = get_session()
    try:
        logging.info(log_str)
        for counter_id in counter_ids if counter_ids else [None]:
            get_table_creator(table_name).create_day(
                dataset, interval, counter_id, session=session
            )
        return True
    except Exception as e:
        logging.exception(
            f"Error collecting data for {log_str}. Error message: {str(e)}"
        )
        return False


def _get_existing_data_for_intervals(
    table_name: str,
    interval_type: str,
    intervals: List[str],
    dataset_name: str = None,
    counter_ids: List[int] = None,
    session: Session = None,
) -> Dict[str, Set[str]]:
    if interval_type == "week":
        from_day, to_day = intervals[0], date_add(intervals[-1], 6)
    elif interval_type == "month":
        from_day, to_day = f"{intervals[0]}-01", get_last_day_of_month(intervals[-1])
    else:
        raise Exception(f"Unknown interval: {interval_type}")
    return get_existing_data(
        table_name,
        from_day,
        to_day,
        dataset_name,
        counter_ids=counter_ids,
        session=session,
    )


def _get_aggregate_interval_range(interval: str, from_day: str, to_day: str):
    if interval == "week":
        if from_day is None and to_day is None:
            return weeks_range(get_last_finished_week(), get_yesterday())
        else:
            return weeks_range(from_day, to_day)
    elif interval == "month":
        if from_day is None and to_day is None:
            return month_range(
                f"{get_last_finished_month()}-01",
                get_last_day_of_month(get_last_finished_month()),
            )
        else:
            return month_range(from_day, to_day)
    else:
        raise Exception(f"Unknown interval: {interval}")


def _get_table_name_by_interval(interval: str) -> str:
    if interval == "week":
        return "weekly_counters"
    elif interval == "month":
        return "monthly_counters"
    else:
        raise Exception(f"Unknown interval: {interval}")


def _daily_data_exists(
    daily_data: Dict[str, Set[str]], dataset: str, interval_type: str, interval: str
) -> bool:
    if interval_type == "week":
        from_day, to_day = interval, date_add(interval, 6)
    elif interval_type == "month":
        from_day, to_day = f"{interval}-01", get_last_day_of_month(interval)
    else:
        raise Exception(f"Unknown interval: {interval_type}")

    missing = set()
    for day in days_range(from_day, to_day):
        if dataset not in daily_data or day not in daily_data[dataset]:
            missing.add(day)
    if len(missing) > 0:
        logging.warning(f"Missing days: {missing}")
    return len(missing) == 0
