import concurrent.futures
import json
import logging
from datetime import date
from typing import NamedTuple, Dict, List, Optional

from boto3 import Session

from athena_utils import QueryStats, run_query, get_query_results
from s3_utils import (
    upload_content_to_s3,
    list_s3_folder_keys,
    get_s3_object_content,
    clear_s3_folder,
    get_session,
    list_s3_folders,
    delete_s3_object,
    get_root_folder,
)
from table_creator import (
    get_table_creators,
    get_table_creator,
    get_database_name,
    _COUNTER_PARTITION_NAME,
)
from temp_table_utils import generate_temp_table_name
from utils import get_max_workers

_DAY_PH = "{day}"
_DEFAULT_MIN_AVG = 10.0
_DEFAULT_DETECT_QUERY_TIMEOUT = 300


class _Counter(NamedTuple):
    id: int
    name: str
    aggregate: bool
    aggregation_method: str
    sql: str
    value: str
    min_avg: float
    max_record: bool
    timeout: int


def replace_sql_place_holders(sql: str, day: str, temp_tables: Dict[str, str]) -> str:
    result = sql.replace(_DAY_PH, day)
    for temp_table in temp_tables:
        result = result.replace("{" + temp_table + "}", temp_tables[temp_table])
    return result


def build_exists_query(sql, day: str) -> str:
    return f"SELECT COUNT() > 0 AS result WHERE EXISTS ({replace_sql_place_holders(sql, day, {})})"


class _CounterDataset:
    def __init__(self, name: str) -> None:
        super().__init__()
        self.name = name
        self.counters: Dict[int, _Counter] = {}
        self.temp_tables: Dict[str, str] = {}
        self.views: Dict[str, str] = {}
        self.prerequisites: List[str] = []
        self.detection_methods: List[str] = []
        self.query_timeout: Dict[str, int] = {}


_DATASETS: Dict[str, _CounterDataset] = {}
_listed = False


def _dataset_from_file(dataset_file: str) -> _CounterDataset:
    with open(dataset_file) as f:
        dataset_json = json.load(f)
    return _dataset_from_json(dataset_json)


def get_datasets(session: Session = None) -> Dict[str, _CounterDataset]:
    global _listed
    if not _listed:
        _load_datasets_from_s3(session=session)
        _listed = True
    return _DATASETS


def get_dataset(name: str, session: Session = None) -> _CounterDataset:
    if name not in _DATASETS:
        _load_dataset_from_s3(name, session=session)
    return _DATASETS[name]


def _dataset_from_str(json_str: str) -> _CounterDataset:
    return _dataset_from_json(json.loads(json_str))


def _dataset_from_json(dataset_json) -> _CounterDataset:
    dataset = _CounterDataset(dataset_json["dataset"])
    for counter_json in dataset_json["counters"]:
        c_id = counter_json["id"]
        dataset.counters[c_id] = _Counter(
            c_id,
            counter_json["name"],
            counter_json.get("aggregate", True),
            counter_json.get("method", "sum"),
            counter_json["sql"],
            counter_json.get("value"),
            counter_json.get("min-avg", _DEFAULT_MIN_AVG),
            counter_json.get("max-record", False),
            counter_json.get("timeout", _DEFAULT_DETECT_QUERY_TIMEOUT),
        )
    if "temp-tables" in dataset_json:
        for temp_table_name in dataset_json["temp-tables"]:
            dataset.temp_tables[temp_table_name] = dataset_json["temp-tables"][
                temp_table_name
            ]
    if "views" in dataset_json:
        for view_name in dataset_json["views"]:
            dataset.views[view_name] = dataset_json["views"][view_name]

    if "prerequisites" in dataset_json:
        for prerequisite in dataset_json["prerequisites"]:
            dataset.prerequisites.append(prerequisite)

    if "detection-methods" in dataset_json:
        for detection_method in dataset_json["detection-methods"]:
            dataset.detection_methods.append(detection_method)

    return dataset


def get_datasets_folder() -> str:
    return f"{get_root_folder()}/datasets"


def _persist_dataset(
    dataset_name: str, json_data: str, session: Session = None
) -> List[str]:
    return upload_content_to_s3(
        f"{get_datasets_folder()}/{dataset_name}.json", json_data, session
    )


def _validate_dataset_json(json_data) -> List[str]:
    errors = []
    duplicate_ids = []
    duplicate_names = []
    ids = set()
    names = set()
    for counter_json in json_data["counters"]:
        c_errors = validate_counter_json(counter_json)
        if len(c_errors) > 0:
            errors += c_errors
        if "id" in counter_json and "name" in counter_json:
            c_id = counter_json["id"]
            c_name = counter_json["name"]
            if c_id in ids:
                duplicate_ids.append(c_id)
            else:
                ids.add(c_id)
            if c_name in names:
                duplicate_names.append(c_name)
            else:
                names.add(c_name)
    if len(duplicate_ids) > 0:
        errors.append(f"Duplicate ids found: {duplicate_ids}")
    if len(duplicate_names) > 0:
        errors.append(f"Duplicate names found: {duplicate_names}")
    return errors


def validate_counter_json(counter: dict) -> List[str]:
    errors = []
    if "id" not in counter:
        errors.append("Missing id")
    elif not isinstance(counter["id"], int) or counter["id"] < 0 or counter["id"] > 255:
        errors.append(
            f"Invalid id: {counter['id']}. Should be an integer between 0 and 255"
        )
    if len(errors) > 0:
        return errors
    if "name" not in counter:
        errors.append("Missing name")
    if "sql" not in counter:
        errors.append("Missing sql")
    if "aggregate" in counter and not isinstance(counter["aggregate"], bool):
        errors.append("aggregate should be boolean")
    if "method" in counter and counter["method"] not in ("sum", "max"):
        errors.append(f"Invalid aggregation method: {counter['method']}")
    if "min-avg" in counter:
        try:
            value = float(counter["min-avg"])
            if value < 0:
                errors.append(
                    f"min-avg should be non-negative. Got: {counter['min-avg']}"
                )
        except (TypeError, ValueError):
            errors.append(f"min-avg should be a number. Got: {counter['min-avg']}")
    if "max-record" in counter and not isinstance(counter["max-record"], bool):
        errors.append("max-record should be boolean")
    if "timeout" in counter:
        timeout = counter["timeout"]
        if not isinstance(timeout, int):
            try:
                timeout = int(timeout)
            except (TypeError, ValueError):
                errors.append(
                    f"timeout should be a positive integer. Got: {counter['timeout']}"
                )
                return errors
        if timeout <= 0:
            errors.append("timeout should be a positive integer")
    return [f"Counter ID: {counter['id']} - {e}" for e in errors]


def update_dataset(json_data: dict) -> Dict:
    errors = _validate_dataset_json(json_data)
    if len(errors) > 0:
        return {"errors": errors}
    dataset: _CounterDataset = _dataset_from_json(json_data)
    errors = validate_dataset(dataset)
    if len(errors) > 0:
        return {"errors": errors}
    _DATASETS[dataset.name] = dataset
    session = get_session()
    errors = _persist_dataset(
        dataset.name, json.dumps(json_data, indent=4), session=session
    )
    if len(errors) > 0:
        return {"errors": errors}
    get_table_creator("counters_metadata").create_day(dataset.name, "", session=session)
    logging.info(f"Added or updated dataset: {dataset.name}")
    return {"status": "OK", "dataset": dataset.name, "counters": len(dataset.counters)}


def _load_datasets_from_s3(session: Session = None):
    datasets_objects = list_s3_folder_keys(f"{get_datasets_folder()}/", session)
    for dataset_key in datasets_objects:
        _load_dataset_by_key(dataset_key, session=session)
    logging.info(f"Loaded {len(_DATASETS)} datasets: {[d for d in _DATASETS]}")


def _load_dataset_from_s3(name: str, session: Session = None) -> _CounterDataset:
    try:
        return _load_dataset_by_key(
            f"{get_datasets_folder()}/{name}.json", session=session
        )
    except Exception as e:
        raise Exception(f"Could not find dataset: {name}. Error: {str(e)}")


def _load_dataset_by_key(key: str, session: Session = None) -> _CounterDataset:
    obj_data = get_s3_object_content(key, session)
    dataset = _dataset_from_str(obj_data)
    _DATASETS[dataset.name] = dataset
    if logging.getLogger().isEnabledFor(logging.DEBUG):
        logging.debug(f"Loaded dataset: {dataset.name}")
    return dataset


def create_dataset_temp_tables(
    dataset: _CounterDataset, day: str, query_stats: QueryStats, session: Session = None
):
    result = {}
    for temp_table in dataset.temp_tables:
        temp_table_name = _create_dataset_temp_table(
            dataset, day, query_stats, temp_table, session=session
        )
        result[temp_table] = temp_table_name
    return result


def _create_dataset_temp_table(
    dataset: _CounterDataset, day, query_stats, temp_table, session: Session = None
) -> str:
    temp_table_name = generate_temp_table_name(day)
    logging.info(
        f"Going to create a temp table {temp_table} for dataset {dataset.name}. Name: {temp_table_name}"
    )
    temp_table_sql = replace_sql_place_holders(dataset.temp_tables[temp_table], day, {})
    result = run_query(
        f"CREATE TABLE {temp_table_name} AS {temp_table_sql}",
        session=session,
        query_stats=query_stats,
    )
    if result["Status"] != "SUCCEEDED":
        if "StateChangeReason" in result:
            raise Exception(result["StateChangeReason"])
        else:
            raise Exception(result)
    return temp_table_name


def _create_dataset_view(dataset, day, query_stats, view, session: Session = None):
    view_name = f"{get_database_name()}.{dataset.name}_{view}"
    logging.info(f"Going to create a dataset view: {view_name}")
    temp_table_sql = replace_sql_place_holders(dataset.views[view], day, {})
    result = run_query(f"DROP VIEW IF EXISTS {view_name}", session=session)
    if result["Status"] != "SUCCEEDED":
        if "StateChangeReason" in result:
            raise Exception(result["StateChangeReason"])
        else:
            raise Exception(result)
    result = run_query(
        f"CREATE VIEW {view_name} AS {temp_table_sql}",
        query_stats=query_stats,
        session=session,
    )
    if result["Status"] != "SUCCEEDED":
        if "StateChangeReason" in result:
            raise Exception(result["StateChangeReason"])
        else:
            raise Exception(result)


def _compile_counter(
    counter: _Counter, day: str, temp_tables: Dict[str, str], query_stats: QueryStats
) -> List[str]:
    sql = replace_sql_place_holders(counter.sql, day, temp_tables)
    errors = []
    try:
        session = get_session()
        results = get_query_results(sql, query_stats, session=session)
        cols = {
            c["Name"]: c["Type"]
            for c in results["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]
        }
        if (
            "str_key" not in cols
            or "int_key" not in cols
            or "value" not in cols
            or len(cols) != 3
        ):
            errors.append(
                f"Expected 3 columns: int_key, str_key, value. Got {list(cols.keys())}"
            )
        for field in ["str_key", "int_key"]:
            if field in cols and cols[field] != "array":
                errors.append(f"{field} data type should be array. Got {cols[field]}")
        if "value" in cols and cols["value"] not in ["integer", "bigint"]:
            errors.append(f"value data type should be integer. Got {cols['value']}")
    except Exception as e:
        errors.append(
            f"Counter: {counter.name}, SQL Error: {_format_athena_error(str(e))}"
        )
    return errors


def _format_athena_error(error: str) -> str:
    if "StartQueryExecution operation: " in error:
        return error[
            error.find("StartQueryExecution operation: ")
            + len("StartQueryExecution operation: ") :
        ]
    else:
        return error


def validate_dataset(dataset: _CounterDataset) -> List[str]:
    logging.info(
        f"Going to validate dataset: {dataset.name}. Counters number: {len(dataset.counters)}"
    )
    result = []
    query_stats = QueryStats()
    session = get_session()

    today = str(date.today())
    temp_tables = {}
    for temp_table in dataset.temp_tables:
        try:
            temp_tables[temp_table] = _create_dataset_temp_table(
                dataset, today, query_stats, temp_table, session=session
            )
        except Exception as e:
            return [
                f"Error creating temp table: {temp_table}. Message: {_format_athena_error(str(e))}"
            ]
    for view in dataset.views:
        try:
            _create_dataset_view(dataset, today, query_stats, view, session=session)
        except Exception as e:
            return [
                f"Error creating view: {view}. Message: {_format_athena_error(str(e))}"
            ]
    logging.info(f"Validating prerequisites: {len(dataset.prerequisites)}")
    for pre_req in dataset.prerequisites:
        try:
            pre_req = build_exists_query(pre_req, today)
            pre_req_result = get_query_results(
                pre_req, session=session, query_stats=query_stats
            )
            if len(pre_req_result["ResultSet"]["Rows"]) != 2 or pre_req_result[
                "ResultSet"
            ]["Rows"][1]["Data"][0]["VarCharValue"] not in ["true", "false"]:
                return [
                    f"Error running prerequisite: {pre_req}. Result is in invalid format: {pre_req_result}"
                ]
        except Exception as e:
            return [
                f"Error running prerequisite: {pre_req}. Message: {_format_athena_error(str(e))}"
            ]

    for c in dataset.counters.values():
        workers = get_max_workers()
        logging.info(
            f"Validating counter. Id: {c.id}, Name: {c.name}. Max workers: {workers}"
        )
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=workers, thread_name_prefix="detect_"
        ) as thread_pool:
            futures = []
            future = thread_pool.submit(
                _compile_counter, c, today, temp_tables, query_stats
            )
            futures.append(future)
            for f in concurrent.futures.as_completed(futures):
                errors = f.result()
                if len(errors) > 0:
                    result += errors
                    break
    for temp_table in temp_tables.values():
        run_query(f"DROP TABLE IF EXISTS {temp_table}", session=session)
    logging.info(f"Data scanned: {query_stats}")
    return result


def clear_dataset_data(
    dataset_name: str,
    table_name: Optional[str],
    from_interval: Optional[str],
    counter_id: Optional[int],
) -> int:
    deleted = 0
    tables = [t.table_name() for t in get_table_creators() if t.table_name()]
    remove_dataset = table_name is None and from_interval is None
    if not remove_dataset:
        tables.remove("counters_metadata")
    if table_name is not None:
        if table_name in tables:
            tables = [table_name]
        else:
            raise Exception(f"Unknown table: {table_name}")
    session = get_session()
    for table_location in [
        t.table_location() for t in get_table_creators() if t.table_name() in tables
    ]:
        folder = f"{table_location}/dataset={dataset_name}/"
        if from_interval is None:
            logging.info(f"Going to clear dataset table data. Folder: {folder}")
            table_deleted = clear_s3_folder(folder, session=session)
        else:
            table_deleted = 0
            sub_folders = list_s3_folders(folder, session)
            logging.info(
                f"Going to clear dataset table data. Folder: {folder}, from: {from_interval}, "
                f"Number of sub folders: {len(sub_folders) if sub_folders else 0}"
            )
            for interval_folder in sorted(sub_folders) if sub_folders else []:
                interval = interval_folder[interval_folder.rfind("=") + 1 : -1]
                if interval >= from_interval:
                    if counter_id is not None:
                        interval_folder = (
                            f"{interval_folder}{_COUNTER_PARTITION_NAME}={counter_id}/"
                        )
                    logging.info(f"Current folder: {interval_folder}")
                    table_deleted += clear_s3_folder(interval_folder)
        logging.info(f"Deleted {table_deleted} objects from folder: {folder}")
        deleted += table_deleted
    if remove_dataset:
        key = f"{get_datasets_folder()}/{dataset_name}.json"
        logging.info(f"Going to delete key: {key}")
        delete_s3_object(key=key, session=session)
    return deleted
