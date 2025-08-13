import json
import logging
import os
from base64 import b64decode

import boto3
import botocore.config
import pytest

from conftest import get_b64_resource, init_aws_creds


@pytest.fixture(autouse=True, scope="module")
def init_env():
    logging.info("Init env")
    init_aws_creds()


@pytest.fixture(autouse=True)
def set_evn(monkeypatch):
    monkeypatch.setenv("FUNCTION_NAME", "countdb")


class TestLambdaIntegrationDataset:
    @staticmethod
    def invoke_lambda(event: dict) -> dict:
        lambda_client = boto3.client(
            "lambda", config=botocore.config.Config(read_timeout=900)
        )
        response = lambda_client.invoke(
            FunctionName=os.environ["FUNCTION_NAME"],
            Payload=json.dumps(event),
            LogType="Tail",
        )
        if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            raise Exception(f"Error response from lambda: {response}", response)
        else:
            logging.info(
                f"{'=' * 40}LOGS{'=' * 40}\n{b64decode(response['LogResult']).decode('UTF8')}"
            )
            return json.loads(response["Payload"].read())

    def test_lambda_upload(self):
        result = self.invoke_lambda(
            {
                "operation": "upload",
                "data": get_b64_resource("simple_dataset.json").decode(),
            }
        )
        assert "errors" not in result

    def test_lambda_collect(self):
        result = self.invoke_lambda(
            {"operation": "collect", "dataset": "simple_dataset", "override": False}
        )
        assert result["exists"] + result["success"] == 1, result
