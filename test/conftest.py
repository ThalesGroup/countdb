import base64
import os
from pathlib import Path

from moto.athena.models import athena_backends
from moto.core import DEFAULT_ACCOUNT_ID

from athena_backend_infra import MyAthenaBackend


def get_project_folder() -> str:
    return str(Path(os.path.dirname(os.path.abspath(__file__))).parent.absolute())


def init_athena_mock(region: str) -> MyAthenaBackend:
    athena_backend = MyAthenaBackend(region, DEFAULT_ACCOUNT_ID)
    athena_backends[DEFAULT_ACCOUNT_ID][athena_backend.region_name] = athena_backend
    return athena_backend


def _get_resources_dir() -> str:
    return os.path.join(os.path.dirname(__file__), "resources")


def get_resource(name: str) -> str:
    return os.path.join(_get_resources_dir(), name)


def get_b64_resource(name: str):
    with open(get_resource(name)) as f:
        return base64.b64encode(f.read().encode("utf-8"))


def init_aws_creds():
    creds_file = os.path.join(get_project_folder(), "aws", "config")
    if os.path.exists(creds_file):
        os.environ["AWS_CONFIG_FILE"] = creds_file

