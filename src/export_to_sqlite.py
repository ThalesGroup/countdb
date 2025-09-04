import gzip
import json
import logging
import os
import tempfile
from abc import ABC, abstractmethod

from typing import List, Dict, Any

from athena_utils import run_ctas_query
from s3_utils import (
    download_s3_file,
    upload_content_to_s3,
    get_root_folder,
    get_s3_object_content,
    upload_file_to_s3,
    obj_exists,
)
from table_creator import get_database_name
from utils import get_yesterday, get_session

_DB_FILENAME = "data.db"


def _create_data_filter(config: dict, key_col_condition: str, key_col_exp: str) -> str:
    result = ""
    for dataset in config["datasets"]:
        counter_ids = [str(c["id"]) for c in dataset["counters"] if "filter" not in c]
        if len(counter_ids) > 0:
            result += f"\n  OR (dataset = '{dataset['name']}' AND counter_id IN ({','.join(counter_ids)}))"
        for c in [c for c in dataset["counters"] if "filter" in c]:
            result += f"\n  OR (dataset = '{dataset['name']}'"
            filter_str = str(config["filters"][c["filter"]]["original"])[1:-1]
            result += f" AND counter_id = {c['id']} AND {key_col_condition} AND {key_col_exp} IN ({filter_str}))"
    if len(result) > 0:
        result = result[6:]  # remove first OR
    return result


class TableImporter(ABC):
    def __init__(self, table_name: str, config: dict):
        self.table_name = table_name
        self.config = config
        self.filter_config = self.calc_filter_config()

    def calc_filter_config(self):
        result = {d["name"]: {} for d in self.config["datasets"]}
        for d in self.config["datasets"]:
            for c in d["counters"]:
                if "filter" in c and c["filter"]:
                    result[d["name"]][c["id"]] = self.config["filters"][c["filter"]]
        return result

    @property
    def task_name(self) -> str:
        return self.config["export-task"]

    def get_property(self, prop_name: str, default=None) -> str:
        return self.config.get("properties", {}).get(prop_name, default)

    @property
    @abstractmethod
    def columns(self) -> Dict[str, type]:
        return {
            "counter_id": int,
            "dataset": str,
        }

    @abstractmethod
    def data_filter(self) -> str:
        raise NotImplementedError()

    @property
    def bucketed_field(self) -> str:
        return "counter_id"

    def get_time_filter(self, to_day: str) -> str:
        return "TRUE"

    def update_line(self, line: Dict[str, Any]) -> dict[str, Any]:
        if line["counter_id"] in self.filter_config[line["dataset"]]:
            self.update_key(
                line,
                self.filter_config[line["dataset"]][line["counter_id"]],
            )
        return line

    @abstractmethod
    def update_key(self, line: dict, f: dict):
        raise NotImplementedError()

    def run(
        self,
        db_filename: str,
        to_day: str,
        force: bool = False,
        records_limit: int = None,
        session=None,
        verbose: bool = False,
    ) -> int:
        logging.info(f"Running import for table {self.table_name}")
        filter_condition = self.data_filter()
        time_filter = self.get_time_filter(to_day)
        logging.info(f"Time filter: {time_filter}")
        logging.info(f"Data filter:\n{filter_condition}")
        query = f"""SELECT {",".join(self.columns)} 
FROM {get_database_name()}.{self.table_name} 
WHERE {time_filter} 
      AND {filter_condition} 
ORDER BY dataset, counter_id
LIMIT {records_limit if records_limit else 'ALL'}"""
        if verbose:
            logging.info(f"Query for table {self.table_name}:\n{query}")
        keys = run_ctas_query(
            f"{self.task_name}_{self.table_name}",
            query,
            self.bucketed_field,
            force=force,
            session=session,
        )
        output_dir = os.path.join(tempfile.gettempdir(), self.task_name)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        local_files = []
        for key in keys:
            key_path = os.path.join(output_dir, key.replace("/", "_"))
            if os.path.exists(key_path):
                if force:
                    os.remove(key_path)
                    download_s3_file(key, key_path)
            else:
                download_s3_file(key, key_path)
            local_files.append(key_path)
        records = 0
        for local_file in local_files:
            with gzip.open(local_file, "rt") as f:
                import sqlite_utils

                db = sqlite_utils.Database(db_filename)
                db[self.table_name].create(self.columns, pk=None, replace=True)
                logging.info(
                    f"Created table {self.table_name}. Inserting data from {local_file}"
                )
                tbl = db[self.table_name].insert_all(
                    (self.update_line(json.loads(line)) for line in f),
                    batch_size=5000,
                    alter=True,
                )
                records += tbl.count
                logging.info(
                    f"Finished inserting data for table {self.table_name}. records: {records}"
                )
        return records


class CountersMetadata(TableImporter):
    def __init__(self, config: dict):
        super().__init__("counters_metadata", config)

    @property
    def columns(self) -> Dict[str, type]:
        result = super().columns
        result.update(
            {
                "counter_name": str,
                "value_type": str,
            }
        )
        return result

    def data_filter(self) -> str:
        result = ""
        for dataset in self.config["datasets"]:
            counter_ids = [str(c["id"]) for c in dataset["counters"]]
            result += f"\n  OR (dataset = '{dataset['name']}' AND counter_id IN ({','.join(counter_ids)}))"
        if len(result) > 0:
            result = result[6:]  # remove first OR
        return result

    def update_key(self, line: dict, f: dict):
        pass


class TimeCounters(TableImporter, ABC):
    def __init__(self, table_name: str, config: dict):
        super().__init__(table_name, config)

    @property
    @abstractmethod
    def time_column(self) -> str:
        raise NotImplementedError()

    @property
    def columns(self) -> Dict[str, type]:
        result = super().columns
        result.update(
            {
                "int_key1": int,
                "int_key2": int,
                "str_key1": str,
                "str_key2": str,
                "value": int,
            }
        )
        result[self.time_column] = str
        return result

    def data_filter(self) -> str:
        return _create_data_filter(self.config, "TRUE", "int_key1")

    def update_key(self, line: dict, f: dict):
        line["int_key1"] = f["mapped"][f["original"].index(line["int_key1"])]


class DailyCounters(TimeCounters):
    def __init__(self, config: dict):
        super().__init__("daily_counters", config)

    @property
    def time_column(self) -> str:
        return "day"

    def get_time_filter(self, to_day: str) -> str:
        days = self.get_property("days", "90")
        return f"DATE(day) BETWEEN DATE_ADD('day', -{days}, DATE('{to_day}')) AND DATE('{to_day}')"


class WeeklyCounters(TimeCounters):
    def __init__(self, config: dict):
        super().__init__("weekly_counters", config)

    @property
    def time_column(self) -> str:
        return "week"

    def get_time_filter(self, to_day: str) -> str:
        weeks = self.get_property("weeks", "25")
        return f"DATE(week) BETWEEN DATE_ADD('week', -{weeks}, DATE('{to_day}')) AND DATE('{to_day}')"


class MonthlyCounters(TimeCounters):
    def __init__(self, config: dict):
        super().__init__("monthly_counters", config)

    @property
    def time_column(self) -> str:
        return "month"

    def get_time_filter(self, to_day: str) -> str:
        months = self.get_property("months", 12)
        return f"""month BETWEEN SUBSTRING(CAST(DATE_ADD('month', {months}, DATE('{to_day}')) AS VARCHAR), 1, 7) 
                                 AND SUBSTRING(CAST(DATE('{to_day}') AS VARCHAR), 1, 7)"""


class Highlights(TableImporter):
    def __init__(self, config: dict):
        super().__init__("highlights", config)

    @property
    def columns(self) -> Dict[str, type]:
        result = super().columns
        result.update(
            {
                "interval_type": str,
                "method": str,
                "sub_method": str,
                "int_key": str,
                "str_key": str,
                "anomalies": str,
                "intervals": str,
                "vals": str,
            }
        )
        return result

    def data_filter(self) -> str:
        return _create_data_filter(
            self.config, "CARDINALITY(int_key) > 0", "int_key[1]"
        )

    def update_key(self, line: dict, f: dict):
        value = line["int_key"][0]
        line["int_key"] = [f["mapped"][f["original"].index(value)]]


def get_importers(config: dict) -> List[TableImporter]:
    return [
        CountersMetadata(config),
        DailyCounters(config),
        WeeklyCounters(config),
        MonthlyCounters(config),
        Highlights(config),
    ]


def run_importers(
    config: dict,
    to_day: str,
    tables: List[str] = None,
    force: bool = False,
    records_limit: int = None,
    session=None,
    verbose: bool = False,
) -> dict:
    logging.info(f"Importing data to SQLite database")
    importers = get_importers(config)
    if tables and any(
        True for t in tables if t not in [importer.table_name for importer in importers]
    ):
        raise ValueError(
            f"Invalid table names provided: {tables}. Valid tables are {[importer.table_name for importer in importers]}"
        )
    target_object = f"{get_root_folder()}/exports/name={config['export-task']}/to_day={to_day}/data.db"
    with tempfile.NamedTemporaryFile() as temp_file:
        if obj_exists(target_object, session):
            download_s3_file(target_object, temp_file.name)
        tables_results = {}
        for importer in importers:
            try:
                if tables is None or importer.table_name in tables:
                    records = importer.run(
                        temp_file.name, to_day, force, records_limit, session, verbose
                    )
                    tables_results[importer.table_name] = records
            except Exception as e:
                logging.error(f"Error importing table {importer.table_name}: {e}")
                raise

        upload_file_to_s3(
            temp_file.name,
            target_object,
            session,
        )
    return {
        "path": target_object,
        "tables": tables_results,
        "success": True,
        "target_object": target_object,
    }


def export_to_sqlite(
    task: str,
    to_day: str = None,
    tables: List[str] = None,
    force: bool = False,
    records_limit: int = None,
    verbose: bool = False,
):
    logging.info("STARTING data import to SQLite")
    config_path = f"{get_root_folder()}/export-tasks/{task}.json"
    session = get_session()
    obj_data = get_s3_object_content(config_path, session)
    config = json.loads(obj_data)
    if to_day is None:
        to_day = get_yesterday()
    logging.info("FINISHED data import to SQLite")
    return run_importers(config, to_day, tables, force, records_limit, session, verbose)


def upload_export_task(json_data: dict) -> dict:
    errors = []
    task_name = json_data["export-task"]
    result = {"export-task": task_name}
    if "datasets" not in json_data or len(json_data["datasets"]) == 0:
        errors.append("No datasets provided")
    else:
        upload_errors = upload_content_to_s3(
            f"{get_root_folder()}/export-tasks/{task_name}.json",
            json.dumps(json_data, indent=4),
        )
        if upload_errors:
            errors.extend(upload_errors)
        else:
            result["success"] = True
    if len(errors) > 0:
        result["errors"] = errors
    return result
