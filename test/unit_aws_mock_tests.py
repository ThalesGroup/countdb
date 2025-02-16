import base64
import datetime
import json
import os
import re
from typing import Optional

import pytest
from moto import mock_aws
from moto.athena.models import QueryResults

from athena_backend_infra import AthenaMockQueryHandler, MyAthenaBackendContext
from conftest import get_b64_resource, init_athena_mock
from events import handle_event
from s3_utils import folder_last_modified, upload_content_to_s3
from utils import get_session
from athena_utils import reset_sleep_time_seconds


@pytest.fixture(autouse=True)
def athena_mock(monkeypatch):
    reset_sleep_time_seconds()
    monkeypatch.setenv("BUCKET", "my-bucket")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("ATHENA_LOGS", "temp/countdb/athena")
    monkeypatch.setenv("WORKGROUP", "countdb")
    mock = mock_aws()
    mock.start()
    athena_mock = init_athena_mock(os.environ["AWS_DEFAULT_REGION"])
    session = get_session()
    s3 = session.client("s3")
    s3.create_bucket(Bucket=os.environ["BUCKET"])
    athena = session.client("athena")
    athena.create_work_group(Name=os.environ["WORKGROUP"], Configuration={})
    yield athena_mock
    mock.stop()


class TestCountDBIntegration:
    def test_folder_last_modified(self):
        session = get_session()
        upload_content_to_s3("some/path/to/check/1.txt", "hello", session)
        result = folder_last_modified("some/path/to/check/", session)
        assert result == {"1.txt": str(datetime.date.today())}

    def test_dataset_doesnt_exist(self):
        with pytest.raises(Exception, match="Could not find dataset: no_such_database"):
            handle_event({"operation": "collect", "dataset": "no_such_database"})

    def test_init(self):
        result = handle_event({"operation": "init"})
        assert "OK" == result["result"]

    def test_drop_temp_tables(self):
        result = handle_event({"operation": "drop-temp-tables"})
        assert "dropped-temp-tables" in result
        assert result["dropped-temp-tables"] >= 0

    def test_clear_dataset(self):
        result = handle_event({"operation": "clear", "dataset": "dataset1"})
        assert result == {'deleted': 0, 'operation': 'delete'}
        result = handle_event({"operation": "clear", "dataset": "dataset1", "table": "daily_counters"})
        assert result == {'deleted': 0, 'operation': 'delete'}

    def test_drop_database(self):
        result = handle_event({"operation": "drop-database"})
        assert result["databases"] == ['countdb', 'countdb_temp']

    def test_compile_missing_fields(self):
        bad_json = {"dataset": "bad_dataset",
                    "counters": [
                        {
                            "id": 1,
                            "name": "bad",
                            "sum": True,
                            "sql": "SELECT 1 AS col"
                        }, ]}
        result = handle_event({"operation": "upload",
                               "data": base64.b64encode(json.dumps(bad_json).encode("utf-8"))})
        assert "errors" in result
        assert len(result["errors"]) == 1, result["errors"]
        assert result["errors"][0] == "Expected 3 columns: int_key, str_key, value. Got []", result["errors"][0]


class TestSimpleDataset:
    class SimpleDatasetHandler(AthenaMockQueryHandler):
        @staticmethod
        def get_pattern() -> re.Pattern:
            return re.compile("SELECT ARRAY.*")

        @staticmethod
        def query_match(m: re.Match, exec_params: Optional[list], context: MyAthenaBackendContext):
            return QueryResults([],
                                [{'Name': 'int_key', 'Type': 'array'}, {'Name': 'str_key', 'Type': 'array'},
                                 {'Name': 'value', 'Type': 'integer'}])

    @pytest.fixture()
    def simple_dataset(self, athena_mock):
        athena_mock.add_handler(TestSimpleDataset.SimpleDatasetHandler(), True)
        result = handle_event({"operation": "upload", "data": get_b64_resource("simple_dataset.json")})
        assert "errors" not in result

    def test_simple_counter_single(self, simple_dataset):
        result = handle_event({"operation": "collect", "dataset": "simple_dataset", "counter_id": 1})
        assert {"exists": 0, "operation": "collect", "success": 1}.items() <= result.items()

    def test_simple_counter_multiple(self, simple_dataset):
        result = handle_event({"operation": "collect", "dataset": "simple_dataset"})
        assert {"exists": 0, "operation": "collect", "success": 1}.items() <= result.items()

    def test_simple_counter_full_month(self, simple_dataset):
        from_day, to_day = "2024-05-01", "2024-05-31"
        result = handle_event({"operation": "collect", "dataset": "simple_dataset",
                               "from_day": from_day, "to_day": to_day})
        assert {"exists": 0, "operation": "collect", "success": 31}.items() <= result.items()
        result = handle_event({"operation": "aggregate", "dataset": "simple_dataset",
                               "from_day": from_day, "to_day": to_day})
        assert {"exists": 0, "operation": "aggregate", "missing_days": 4}.items() <= result.items()
        params = {"dataset": "simple_dataset", "operation": "detect", "from_day": from_day, "to_day": to_day,
                  "interval": "month"}
        result = handle_event(params)
        assert {"operation": "detect"}.items() <= result.items()
