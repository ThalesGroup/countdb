import logging
import os

import pytest

from athena_utils import get_query_results_dict, database_exists
from conftest import get_b64_resource, init_aws_creds
from countdb_cli import init_evn_from_config_file
from events import handle_event
from table_creator import get_database_name
from utils import days_range


@pytest.fixture(autouse=True, scope="module")
def init_env():
    logging.info("Init env")
    init_aws_creds()
    init_evn_from_config_file(
        os.path.join(os.path.dirname(__file__), "countdb.config.json")
    )
    if not database_exists(get_database_name()):
        handle_event({"operation": "init"})


@pytest.fixture(autouse=True)
def set_env(monkeypatch):
    monkeypatch.setenv("TEST", "true")


class TestSimpleIntegrationDataset:
    def test_upload(self):
        result = handle_event(
            {"operation": "upload", "data": get_b64_resource("simple_dataset.json")}
        )
        assert "errors" not in result

    def test_recreate_table(self):
        result = handle_event({"operation": "init", "table_name": "counters_metadata"})
        assert "counters_metadata" in result["tables"]
        result = handle_event({"operation": "init", "table_name": "daily_counters"})
        assert "daily_counters" in result["tables"]
        result = handle_event({"operation": "init", "table_name": "highlights"})
        assert "highlights" in result["tables"]
        result = handle_event({"operation": "init", "table_name": "daily_max_counters"})
        assert "daily_max_counters" in result["tables"]

    def test_collect_simple_counter_collect(self):
        result = handle_event(
            {"operation": "collect", "dataset": "simple_dataset", "override": True}
        )
        assert {
            "exists": 0,
            "operation": "collect",
            "success": 1,
        }.items() <= result.items()
        result = handle_event(
            {"operation": "collect", "dataset": "simple_dataset", "override": False}
        )
        assert {
            "exists": 1,
            "operation": "collect",
            "success": 0,
        }.items() <= result.items()

    def test_collect_and_aggregate_full_month(self):
        start_day = "2024-05-01"
        end_day = "2024-05-31"
        result = handle_event(
            {
                "operation": "collect",
                "dataset": "simple_dataset",
                "override": False,
                "from_day": start_day,
                "to_day": end_day,
            }
        )
        assert result["exists"] + result["success"] == 31
        week_start_day = "2024-05-06"
        week_end_day = "2024-05-12"
        result = handle_event(
            {
                "operation": "aggregate",
                "dataset": "simple_dataset",
                "interval": "week",
                "from_day": week_start_day,
                "to_day": week_end_day,
                "override": True,
            }
        )
        assert {
            "exists": 0,
            "operation": "aggregate",
            "success": 1,
        }.items() <= result.items()

        weekly_counters = list(
            get_query_results_dict(
                f"""SELECT counter_id, str_key1, int_key1, value 
                FROM countdb.weekly_counters 
                WHERE dataset = 'simple_dataset' 
                      AND week = '{week_start_day}' 
                ORDER BY counter_id, str_key1, int_key1"""
            )
        )
        assert weekly_counters == [
            {"counter_id": "1", "str_key1": "simple1", "int_key1": None, "value": "7"},
            {"counter_id": "1", "str_key1": "simple2", "int_key1": None, "value": "14"},
            {"counter_id": "2", "str_key1": None, "int_key1": "1", "value": "7"},
            {"counter_id": "2", "str_key1": None, "int_key1": "2", "value": "63"},
            {"counter_id": "3", "str_key1": None, "int_key1": None, "value": "7"},
        ]

        result = handle_event(
            {
                "operation": "aggregate",
                "dataset": "simple_dataset",
                "interval": "week",
                "from_day": start_day,
                "to_day": end_day,
                "override": False,
            }
        )
        assert result["exists"] >= 1
        assert result["exists"] + result["success"] == 3

        result = handle_event(
            {
                "operation": "aggregate",
                "dataset": "simple_dataset",
                "interval": "month",
                "from_day": start_day,
                "to_day": end_day,
                "override": True,
            }
        )
        assert {
            "exists": 0,
            "operation": "aggregate",
            "success": 1,
        }.items() <= result.items()
        monthly_counters = list(
            get_query_results_dict(
                """SELECT str_key1, value 
               FROM countdb.monthly_counters 
               WHERE dataset = 'simple_dataset' 
                     AND month = '2024-05' 
                     AND counter_id = 1
               ORDER BY str_key1"""
            )
        )
        assert monthly_counters == [
            {"str_key1": "simple1", "value": "31"},
            {"str_key1": "simple2", "value": "62"},
        ]

    @staticmethod
    def _run_detect(method: str) -> dict:
        start_day = "2024-05-01"
        end_day = "2024-05-31"

        handle_event(
            {
                "operation": "detect",
                "dataset": "simple_dataset",
                "interval": "day",
                "from_day": start_day,
                "to_day": end_day,
                "override": True,
                "method": method,
            }
        )
        highlights = list(
            get_query_results_dict(
                f"""SELECT *
               FROM countdb.highlights
               WHERE dataset = 'simple_dataset'
                     AND interval_type = 'day'
                     AND counter_id = 2
                     AND method = '{method}'"""
            )
        )
        assert len(highlights) == 1
        assert (
            highlights[0].items()
            >= {
                "method": method,
                "counter_id": "2",
                "intervals": f'[{", ".join(days_range(start_day, end_day))}]',
                "dataset": "simple_dataset",
                "interval_type": "day",
            }.items()
        )
        return highlights[0]

    def test_detect_peak(self):
        result = self._run_detect(method="Peak")
        assert (
            result.items()
            >= {
                "int_key": "[1]",
                "sub_method": "EmpiricalRule",
                "anomalies": "[2024-05-15]",
                "vals": str([100 if i == 15 else 1 for i in range(1, 32)]),
            }.items()
        )

    def test_detect_trend(self):
        result = self._run_detect(method="Trend")
        assert (
            result.items()
            >= {
                "int_key": "[2]",
                "sub_method": None,
                "anomalies": None,
                "vals": str(list(range(1, 32))),
            }.items()
        )

    def test_detect_pattern(self):
        result = self._run_detect(method="Pattern")
        assert (
            result.items()
            >= {
                "sub_method": "RecentIncrease",
                "int_key": "[2]",
                "anomalies": "[2024-05-31]",
                "vals": str(list(range(1, 32))),
            }.items()
        )

    def test_detect_max(self):
        handle_event(
            {
                "operation": "clear",
                "dataset": "simple_dataset",
                "table": "daily_max_counters",
            }
        )
        handle_event(
            {
                "operation": "detect",
                "dataset": "simple_dataset",
                "interval": "day",
                "from_day": "2024-05-01",
                "to_day": "2024-05-10",
                "override": True,
                "method": "max",
            }
        )
        max_records = list(
            get_query_results_dict(
                f"""SELECT counter_id, int_key1, str_key1, value, max_interval, prev_value
               FROM countdb.daily_max_counters
               WHERE dataset = 'simple_dataset'
                     AND day = '2024-05-10'
               ORDER BY counter_id, int_key1, str_key1"""
            )
        )
        assert max_records[0] == {
            "counter_id": "1",
            "int_key1": None,
            "str_key1": "simple1",
            "value": "1",
            "max_interval": "2024-05-01",
            "prev_value": None,
        }
        assert max_records[1] == {
            "counter_id": "1",
            "int_key1": None,
            "str_key1": "simple2",
            "value": "2",
            "max_interval": "2024-05-01",
            "prev_value": None,
        }
        assert max_records[2] == {
            "counter_id": "2",
            "int_key1": "1",
            "str_key1": None,
            "value": "1",
            "max_interval": "2024-05-01",
            "prev_value": None,
        }
        assert max_records[3] == {
            "counter_id": "2",
            "int_key1": "2",
            "str_key1": None,
            "value": "10",
            "max_interval": "2024-05-10",
            "prev_value": "9",
        }
        handle_event(
            {
                "operation": "detect",
                "dataset": "simple_dataset",
                "interval": "day",
                "from_day": "2024-05-11",
                "to_day": "2024-05-20",
                "override": True,
                "method": "max",
            }
        )
        max_records = list(
            get_query_results_dict(
                f"""SELECT counter_id, int_key1, str_key1, value, max_interval, prev_value
               FROM countdb.daily_max_counters
               WHERE dataset = 'simple_dataset'
                     AND day = '2024-05-20'
               ORDER BY counter_id, int_key1, str_key1"""
            )
        )
        assert max_records[0] == {
            "counter_id": "1",
            "int_key1": None,
            "str_key1": "simple1",
            "value": "1",
            "max_interval": "2024-05-01",
            "prev_value": None,
        }
        assert max_records[1] == {
            "counter_id": "1",
            "int_key1": None,
            "str_key1": "simple2",
            "value": "2",
            "max_interval": "2024-05-01",
            "prev_value": None,
        }
        assert max_records[2] == {
            "counter_id": "2",
            "int_key1": "1",
            "str_key1": None,
            "value": "100",
            "max_interval": "2024-05-15",
            "prev_value": "1",
        }
        assert max_records[3] == {
            "counter_id": "2",
            "int_key1": "2",
            "str_key1": None,
            "value": "20",
            "max_interval": "2024-05-20",
            "prev_value": "19",
        }
        assert (
            int(
                list(
                    get_query_results_dict(
                        f"""SELECT COUNT() AS cnt
               FROM countdb.daily_max_counters
               WHERE dataset = 'simple_dataset'
                     AND counter_id = 3"""
                    )
                )[0]["cnt"]
            )
            == 0
        )
