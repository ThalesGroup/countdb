from abc import ABC
from typing import Dict, Set, List

from boto3 import Session

from athena_utils import get_query_results
from table_creator import get_table_creator, TableCreator


class BasCountersCreator(TableCreator, ABC):
    @staticmethod
    def external_table_columns() -> Dict[str, str]:
        return {
            "counter_id": "tinyint",
            "int_key1": "bigint",
            "int_key2": "bigint",
            "str_key1": "string",
            "str_key2": "string",
            "value": "bigint",
        }


def get_existing_data(
    table_name: str,
    from_day: str,
    to_day: str,
    dataset_name: str = None,
    counter_ids: List[int] = None,
    session: Session = None,
) -> Dict[str, Set[str]]:
    creator = get_table_creator(table_name)
    sql = f"""SELECT DISTINCT dataset, {creator.partition_name()}
 FROM {creator.full_table_name()} 
 WHERE {creator.partition_name()} BETWEEN '{creator.partition_value(from_day)}' AND '{creator.partition_value(to_day)}'
{f"AND dataset='{dataset_name}'" if dataset_name else ""}
{f"AND counter_id IN ({str(counter_ids)[1:-1]})" if counter_ids is not None and len(counter_ids) > 0 else ""}"""
    query_result = get_query_results(sql, session=session)
    result = {}
    for row in query_result["ResultSet"]["Rows"][1:]:
        dataset = row["Data"][0]["VarCharValue"]
        value = row["Data"][1]["VarCharValue"]
        if dataset not in result:
            result[dataset] = set()
        result[dataset].add(value)
    return result
