import logging
import os
import threading
from abc import abstractmethod
from typing import Dict, Iterable, Optional

from boto3 import Session

from athena_utils import run_query, QueryStats
from s3_utils import (
    get_bucket,
    clear_s3_folder,
    get_root_folder,
    add_s3_life_cycle_config,
)
from temp_table_utils import generate_temp_table_name, get_temp_database_name
from utils import get_session

_table_creators_initialized = False
_table_creators_lock = threading.Lock()

_DEFAULT_DATABASE_NAME = "countdb"
_COUNTER_PARTITION_NAME = "cnt_id"


def get_database_name() -> str:
    return os.environ.get("DATABASE_NAME", _DEFAULT_DATABASE_NAME)


class TableCreator:
    """
    Abstract base class for creating and managing external tables in AWS Athena.

    This class provides a framework for defining the structure and behavior of external tables,
    including methods for creating, managing, and querying these tables. Subclasses should
    implement the abstract methods to specify the details for specific tables.
    """

    @staticmethod
    def database_name():
        return get_database_name()

    @abstractmethod
    def table_name(self) -> str:
        raise NotImplementedError()

    def full_table_name(self) -> str:
        return f"{self.database_name()}.{self.table_name()}"

    @abstractmethod
    def table_folder(self) -> str:
        raise NotImplementedError()

    @abstractmethod
    def get_sql(
        self,
        dataset: str,
        day: str,
        counter_id: int = None,
        session: Session = None,
        **kwargs,
    ) -> str:
        raise NotImplementedError()

    def partition_name(self) -> Optional[str]:
        raise NotImplementedError()

    def partition_value(self, day: str) -> str:
        return day

    @staticmethod
    @abstractmethod
    def external_table_columns() -> Dict[str, str]:
        raise NotImplementedError()

    def table_location(self) -> str:
        return f"{get_root_folder()}/tables/{self.table_folder()}"

    def get_full_location(self):
        """
        :return: location including bucket name
        """
        return f"s3://{get_bucket()}/{self.table_location()}"

    @abstractmethod
    def life_cycle_days(self) -> int:
        raise NotImplementedError()

    def recreate_external_table(self, session: Session):
        logging.info(f"Recreating {self.full_table_name()}")
        run_query(f"DROP TABLE IF EXISTS {self.full_table_name()}", session=session)
        run_query(self.external_table_sql(), session=session)
        logging.info(f"Repairing {self.full_table_name()}")
        run_query(
            f"MSCK REPAIR TABLE {self.full_table_name()}", timeout=300, session=session
        )
        logging.info(f"Creating views for {self.full_table_name()}")
        self.create_views()
        if self.life_cycle_days() != -1:
            add_s3_life_cycle_config(
                get_bucket(),
                self.table_location(),
                delete_days=self.life_cycle_days(),
                session=session,
            )

    def create_views(self):
        pass

    def external_table_sql(self) -> str:
        if self.partition_name() is None:
            partition_sql = ""
        else:
            partition_sql = f", {self.partition_name()} string"
        return f"""
CREATE EXTERNAL TABLE {self.full_table_name()} ({self.dict_to_columns_str(self.external_table_columns())}) 
PARTITIONED BY (dataset string{partition_sql})
STORED AS Parquet
LOCATION '{self.get_full_location()}'"""

    @staticmethod
    def dict_to_columns_str(cols: Dict[str, str]) -> str:
        result = ""
        for k in cols:
            result += f"{k} {cols[k]}, "
        return result[:-2]

    def add_partition(self, dataset: str, day: str):
        value = self.partition_value(day)
        run_query(
            f"ALTER TABLE {self.full_table_name()} ADD IF NOT EXISTS PARTITION (dataset='{dataset}',"
            f"{self.partition_name()}='{value}')"
        )

    def _partition_path(self, dataset: str, day: str, **kwargs) -> str:
        par_value = self.partition_value(day)
        return f"{self.table_location()}/dataset={dataset}/{self.partition_name()}={par_value}"

    @staticmethod
    def _split_to_counters() -> bool:
        return True

    def should_create(
        self, dataset: str, counter_id: int = None, session: Session = None
    ) -> bool:
        """
        Determines whether data should be created for a given dataset and counter ID.

        Args:
            dataset (str): The name of the dataset.
            counter_id (int, optional): The ID of the counter. Defaults to None.
            session (Session, optional): The boto3 session to use. Defaults to None.

        Returns:
            bool: True if the data should be created, False otherwise.
        """
        return True

    def create_day(
        self,
        dataset: str,
        day: str,
        counter_id: int = None,
        session: Session = None,
        **kwargs,
    ) -> dict:
        if not self.should_create(dataset, counter_id, session):
            return {"result": "OK"}
        query_stats = QueryStats()
        partition_path = self._partition_path(dataset, day, **kwargs)
        if counter_id:
            partition_path += f"/{_COUNTER_PARTITION_NAME}={counter_id}"
        deleted = clear_s3_folder(partition_path, session=session)
        if deleted > 0:
            logging.info(f"Deleted {deleted} objects from {partition_path}")
        temp_table_name = generate_temp_table_name(day)
        temp_tables = None
        sql = None
        try:
            temp_tables = self._create_temp_tables(dataset, day, query_stats, session)
            kwargs["temp_tables"] = temp_tables
            sql = self.get_sql(dataset, self.partition_value(day), counter_id, **kwargs)
            if logging.getLogger().isEnabledFor(logging.DEBUG):
                logging.debug(sql)
            ctas_query = f"""
CREATE TABLE {temp_table_name} 
WITH (external_location='s3://{get_bucket()}/{partition_path}', 
      format='PARQUET', {"" if counter_id is not None or not self._split_to_counters() else
            f"partitioned_by=ARRAY['{_COUNTER_PARTITION_NAME}'],"}
      bucketed_by=ARRAY['counter_id'], bucket_count=1) AS {sql}"""
            result = run_query(
                ctas_query,
                query_stats,
                session=session,
                timeout=kwargs["timeout"] if "timeout" in kwargs else None,
            )
            if result["Status"] != "SUCCEEDED":
                if "StateChangeReason" in result:
                    raise Exception(result["StateChangeReason"])
                else:
                    raise Exception(result)
            logging.info(
                f"Partition path: {partition_path}. Data scanned: {query_stats}"
            )
        except Exception as e:
            logging.error(f"Error running query:\n{sql}")
            raise e
        finally:
            run_query(f"DROP TABLE IF EXISTS {temp_table_name}", session=session)
            if temp_tables:
                for temp_table in temp_tables.values():
                    run_query(f"DROP TABLE IF EXISTS {temp_table}", session=session)
        self.add_partition(dataset, day)
        return {
            "result": "OK",
            "query_stats": str(query_stats),
            "partition_path": partition_path,
        }

    def _create_temp_tables(
        self,
        dataset_name: str,
        day: str,
        query_stats: QueryStats,
        session: Session = None,
    ) -> Dict[str, str]:
        pass


_table_creators: Dict[str, TableCreator] = {}


def _register_table_creator(table_creator: TableCreator):
    _table_creators[table_creator.table_name()] = table_creator


def _init_table_creators():
    global _table_creators_initialized
    if _table_creators_initialized:
        return
    with _table_creators_lock:
        if _table_creators_initialized:
            return
        # ... initialization code ...
        from counters import DailyCountersCreator

        _register_table_creator(DailyCountersCreator())
        from counters_metadata import CountersMetadata

        _register_table_creator(CountersMetadata())
        from max_counters import DailyMaxCounters, WeeklyMaxCounters, MonthlyMaxCounters

        _register_table_creator(DailyMaxCounters())
        _register_table_creator(WeeklyMaxCounters())
        _register_table_creator(MonthlyMaxCounters())
        from aggregate import WeeklyCountersCreator, MonthlyCountersCreator

        _register_table_creator(WeeklyCountersCreator())
        _register_table_creator(MonthlyCountersCreator())
        from detection_methods import HighlightsCreator

        _register_table_creator(HighlightsCreator())
        _table_creators_initialized = True


def get_table_creator(name: str) -> TableCreator:
    _init_table_creators()
    return _table_creators[name]


def get_table_creators() -> Iterable[TableCreator]:
    _init_table_creators()
    return _table_creators.values()


def create_db(args) -> Dict:
    if "table_name" in args:
        table_name = args["table_name"]
    elif "table" in args:
        table_name = args["table"]
    else:
        table_name = None
    tables = []
    session = get_session()
    if table_name:
        logging.info(f"Going to recreate table: {table_name}")
        get_table_creator(table_name).recreate_external_table(session=session)
        tables.append(table_name)
    else:
        logging.info("Going to recreate DB")
        run_query(
            f"CREATE DATABASE IF NOT EXISTS {get_database_name()}", session=session
        )
        run_query(
            f"CREATE DATABASE IF NOT EXISTS {get_temp_database_name()}", session=session
        )
        _init_table_creators()
        for t in _table_creators.values():
            t.recreate_external_table(session=session)
            tables.append(t.table_name())
    return {"result": "OK", "tables": tables}


def _drop_db() -> Dict:
    databases_to_drop = [get_database_name(), get_temp_database_name()]
    for database in databases_to_drop:
        logging.info(f"Going to drop {database}")
        run_query(f"DROP DATABASE IF EXISTS {database} CASCADE")
    return {"result": "OK", "databases": databases_to_drop}
