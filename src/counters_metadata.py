import logging
from typing import Dict, Optional

from boto3 import Session

from athena_utils import run_query
from datasets import get_dataset
from s3_utils import clear_s3_folder
from table_creator import TableCreator


class CountersMetadata(TableCreator):
    def table_name(self) -> str:
        return "counters_metadata"

    def table_folder(self) -> str:
        return "counters-metadata"

    def get_sql(self, dataset: str, day: str, counter_id: int = None, session: Session = None, **kwargs) -> str:
        create_sql = ""
        for c in get_dataset(dataset, session).counters.values():
            if len(create_sql) > 0:
                create_sql += "\nUNION ALL\n"
            create_sql += f"SELECT CAST({c.id} AS TINYINT) AS counter_id, " \
                          f"'{c.name}' AS counter_name, '{c.value if c.value else ''}' AS value_type, " \
                          f"{"NULL" if c.min_avg is None else str(c.min_avg)} AS min_avg, '{dataset}' AS dataset"
        return create_sql

    def partition_name(self) -> Optional[str]:
        return None

    def create_day(self, dataset: str, day: str, counter_id: int = None, session: Session = None, **kwargs) -> dict:
        partition_path = f"{self.table_location()}/dataset={dataset}/"
        deleted = clear_s3_folder(partition_path, session=session)
        if deleted > 0:
            logging.info(f"Deleted {deleted} objects from {partition_path}")
        result = run_query(f"INSERT INTO {self.full_table_name()} {self.get_sql(dataset, '')}",
                           session=session)
        if result["Status"] != "SUCCEEDED":
            if "StateChangeReason" in result:
                raise Exception(result["StateChangeReason"])
            else:
                raise Exception(result)
        return {"result": "OK"}

    @staticmethod
    def external_table_columns() -> Dict[str, str]:
        return {
            "counter_id": "tinyint",
            "counter_name": "string",
            "value_type": "string",
            "min_avg": "float",
        }

    def life_cycle_days(self) -> int:
        return -1

