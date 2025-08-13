import base64
import logging
from typing import Dict

from aggregate import aggregate
from counters import collect
from datasets import update_dataset_str, clear_dataset_data
from detection_methods import detect_highlights
from max_counters import detect_max_records
from table_creator import create_db, _drop_db
from temp_table_utils import drop_temp_tables


def handle_event(event) -> Dict:
    logging.info(f"Event: {event}")
    if "operation" not in event:
        raise Exception("Operation not sent")
    operation = event["operation"]
    if operation == "upload":
        return _do_update_dataset(event)
    if operation == "init":
        return create_db(event)
    elif operation == "collect":
        return _do_collect(event)
    elif operation == "aggregate":
        return _do_aggregate(event)
    elif operation == "detect":
        return _do_detect(event)
    elif operation == "clear":
        return _do_clear_dataset(event)
    elif operation == "drop-database":
        return _drop_db()
    elif operation == "drop-temp-tables":
        return _do_drop_temp_tables(event)
    else:
        return {"error": f"Unknown operation: {operation}"}


def _do_detect(args) -> Dict:
    dataset = args["dataset"] if "dataset" in args else None
    from_day = args["from_day"] if "from_day" in args else None
    to_day = args["to_day"] if "to_day" in args else None
    interval = args["interval"] if "interval" in args else None
    method = args["method"] if "method" in args else None
    counter_id = args["counter"] if "counter" in args else None
    override = "override" in args and str(args["override"]).lower() == "true"
    if method is None or method == "max":
        detect_max_records(
            dataset_name=dataset,
            interval_type=interval,
            counter_id=counter_id,
            override=override,
            from_day=from_day,
            to_day=to_day,
        )
    if method is None or method in ["Peak", "Trend", "Pattern"]:
        detect_highlights(
            dataset_name=dataset,
            from_day=from_day,
            to_day=to_day,
            interval_type=interval,
            method=method,
            override=override,
        )

    return {"operation": "detect", "status": "UP"}


def _validate_args(args, args_names_to_validate):
    missing = []
    for arg_name in args_names_to_validate:
        if arg_name not in args:
            missing.append([arg_name])
    if len(missing) > 0:
        raise Exception(f"Missing required parameter: {missing}")


def _do_aggregate(args) -> Dict:
    if "day" in args:
        from_day = args["day"]
        to_day = args["day"]
    elif "from_day" in args and "to_day" in args:
        from_day = args["from_day"]
        to_day = args["to_day"]
    else:
        from_day = None
        to_day = None
    interval = args["interval"] if "interval" in args else None
    counter_id = int(args["counter"]) if "counter" in args else None
    override = "override" in args and str(args["override"]).lower() == "true"
    return aggregate(
        from_day,
        to_day,
        dataset_name=args["dataset"] if "dataset" in args else None,
        interval_type=interval,
        override=override,
        counter_ids=[counter_id] if counter_id else None,
    )


def _do_collect(args) -> Dict:
    if "day" in args:
        from_day = args["day"]
        to_day = args["day"]
    elif "from_day" in args and "to_day" in args:
        from_day = args["from_day"]
        to_day = args["to_day"]
    elif "from_day" in args or "to_day" in args:
        raise Exception("Date range invalid")
    else:
        from_day = None
        to_day = None
    override = "override" in args and str(args["override"]).lower() == "true"
    return collect(
        from_day,
        to_day,
        dataset_name=args["dataset"] if "dataset" in args else None,
        override=override,
        counter_id=int(args["counter"]) if "counter" in args else None,
    )


def _do_update_dataset(event: dict):
    conf_data = base64.b64decode(event["data"]).decode()
    return update_dataset_str(conf_data)


def _do_clear_dataset(json_data):
    dataset_name = json_data["dataset"]
    table_name = json_data.get("table")
    from_interval = json_data.get("from_day")
    counter_id = json_data.get("counter")
    deleted = clear_dataset_data(dataset_name, table_name, from_interval, counter_id)
    return {"deleted": deleted, "operation": "delete"}


def _do_drop_temp_tables(json_data) -> Dict:
    try:
        dropped = drop_temp_tables(json_data)
        return {"operation": "clean-temp", "dropped-temp-tables": dropped}
    except Exception as e:
        return {"operation": "clean-temp", "error": str(e)}
