import re
from abc import ABC
from typing import Dict, NamedTuple, Any, List, Optional

from moto.athena.models import QueryResults, Execution, WorkGroup, AthenaBackend


class _MyAthenaBackendExternalTable(NamedTuple):
    partition_by: Dict[str, str]
    bucket: str
    location: str
    properties: Dict[str, Any]


class _MyAthenaBackendCtasTable(NamedTuple):
    columns: Dict[str, str]


class MyAthenaBackendContext(NamedTuple):
    external_tables: Dict[str, _MyAthenaBackendExternalTable] = {}
    ctas_tables: Dict[str, _MyAthenaBackendCtasTable] = {}


class AthenaMockQueryHandler(ABC):

    @staticmethod
    def get_pattern() -> re.Pattern:
        raise NotImplementedError()

    @staticmethod
    def full_match() -> bool:
        return False

    def run_query(self, query: str, exec_params: Optional[List], context: MyAthenaBackendContext):
        m = self.get_pattern().fullmatch(query) if self.full_match() else self.get_pattern().match(query)
        if m is not None:
            return self.query_match(m, exec_params, context)

    @staticmethod
    def query_match(m: re.Match, exec_params: Optional[List], context: MyAthenaBackendContext):
        raise NotImplementedError()


class MyAthenaBackend(AthenaBackend):

    def __init__(self, region_name: str, account_id: str):
        super().__init__(region_name, account_id)
        self._context = MyAthenaBackendContext()
        self._start_query_handlers: List[AthenaMockQueryHandler] = []
        self._query_result_handlers: List[AthenaMockQueryHandler] = []

    def get_query_results(self, exec_id: str) -> QueryResults:
        query_exec: Execution = self.get_query_execution(exec_id)
        for handler in self._query_result_handlers:
            result = handler.run_query(query_exec.query,
                                       query_exec.execution_parameters,
                                       self._context)
            if result:
                return result
        return super().get_query_results(exec_id)

    def start_query_execution(self, query: str, context: str, config: Dict[str, Any], workgroup: WorkGroup,
                              execution_parameters: Optional[List[str]]) -> str:
        e_id = super().start_query_execution(query, context, config, workgroup, execution_parameters)
        for handler in self._start_query_handlers:
            handler.run_query(query, execution_parameters, self._context)
        return e_id

    def add_handler(self, query_handler: AthenaMockQueryHandler, has_result: bool):
        if has_result:
            self._query_result_handlers.append(query_handler)
        else:
            self._start_query_handlers.append(query_handler)

    @property
    def _url_module(self) -> Any:
        backend_module = AthenaBackend.__module__
        backend_urls_module_name = backend_module.replace("models", "urls")
        backend_urls_module = __import__(
            backend_urls_module_name, fromlist=["url_bases", "url_paths"]
        )
        return backend_urls_module



