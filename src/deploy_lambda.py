import json
import os
import tempfile
from random import randint
from time import sleep
from typing import Dict
from zipfile import ZipFile

import boto3
from botocore.exceptions import ClientError

_DEP_PACKAGE = os.path.join(tempfile.gettempdir(), "deployment_package.zip")
_REQUIRED_ENV_VARS = ["BUCKET", "WORKGROUP", "ATHENA_LOGS"]
_OPTIONAL_ENV_VARS = ["ROOT_FOLDER", "DATABASE_NAME", "TEMP_DATABASE_NAME"]
_DEFAULT_FUNCTION_NAME = "countdb"


def _get_root_dir() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir))


def _get_sources_dir() -> str:
    return os.path.join(_get_root_dir(), "src")


def _zip_sources():
    if os.path.exists(_DEP_PACKAGE):
        os.remove(_DEP_PACKAGE)
    with ZipFile(_DEP_PACKAGE, "w") as zfile:
        print("Sources dir: " + _get_sources_dir())
        for f in os.listdir(_get_sources_dir()):
            if f not in ["lambda_function.py", "countdb_cli.py", "deploy_lambda.py"]:
                zfile.write(os.path.join(_get_sources_dir(), f), f)
        with open(os.path.join(_get_sources_dir(), "lambda_function.py"), "r") as f:
            lambda_main_code = f.read()
        with tempfile.NamedTemporaryFile() as tf:
            tf.write(str.encode(lambda_main_code.replace("$VERSION$", _get_version())))
            tf.flush()
            zfile.write(tf.name, "lambda_function.py")


def _get_version():
    with open(os.path.join(_get_root_dir(), "app.yaml"), "r") as f:
        version = f.readlines()[-1].split(":")[1].strip()
    return version


def _generate_random_id() -> str:
    return str(randint(1_000_000, 9_999_999))


def _get_lambda_arn() -> str:
    sts_client = boto3.client("sts")
    region = os.environ["AWS_DEFAULT_REGION"]
    account_id = sts_client.get_caller_identity()["Account"]
    return f"arn:aws:lambda:{region}:{account_id}:function:{_get_function_name()}"


def _create_rule(rule_name: str, cron_exp: str, operation: str, interval: str = None):
    client = boto3.client("events")
    lambda_arn = _get_lambda_arn()
    rule_name = f"{_get_function_name()}-{rule_name}"
    client.put_rule(
        Name=rule_name,
        ScheduleExpression=f'cron({cron_exp})',
        State='ENABLED',
    )
    json_data = {"operation": operation}
    if interval:
        json_data["interval"] = interval
    client.put_targets(Rule=rule_name,
                       Targets=[
                           {
                               "Arn": lambda_arn,
                               "Id": rule_name,
                               "Input": json.dumps(json_data)
                           }]
                       )
    lambda_client = boto3.client("lambda")
    sts_client = boto3.client("sts")
    region = os.environ["AWS_DEFAULT_REGION"]
    account_id = sts_client.get_caller_identity()["Account"]
    lambda_client.add_permission(FunctionName=lambda_arn,
                                 StatementId=_generate_random_id(),
                                 Action='lambda:InvokeFunction',
                                 Principal='events.amazonaws.com',
                                 SourceArn=f"arn:aws:events:{region}:{account_id}:rule/{rule_name}")


def deploy(update_config: bool = False, update_code: bool = True, env_vars: Dict[str, str] = None):
    if env_vars is None:
        env_vars = {v: os.environ[v] for v in _REQUIRED_ENV_VARS}
    client = boto3.client("lambda")
    for env_var_name in _REQUIRED_ENV_VARS:
        if env_var_name not in env_vars:
            raise Exception(f"Missing env var: {env_var_name}")
    for env_var_name in _OPTIONAL_ENV_VARS:
        if env_var_name in os.environ:
            env_vars[env_var_name] = os.environ[env_var_name]
    env_data = {"Variables": env_vars}
    lambda_timeout = 600
    description = "aws:states:opt-out"
    if not function_exists():
        print("Function does not exist. Creating it")
        _zip_sources()
        with open(_DEP_PACKAGE, "rb") as file:
            client.create_function(
                FunctionName=_get_function_name(),
                Runtime="python3.12",
                Role=os.environ["LAMBDA_ROLE"],
                Description=description,
                Environment=env_data,
                Timeout=lambda_timeout,
                Code={
                    "ZipFile": file.read()
                },
                Handler="lambda_function.lambda_handler",
                Publish=True,
                Architectures=["arm64"]
            )
        _update_scheduling()
        return
    if update_config:
        print("Function exists. Updating config")
        client.update_function_configuration(
            FunctionName=_get_function_name(),
            Description=description,
            Environment=env_data,
            Timeout=lambda_timeout,
        )
    if update_code:
        _zip_sources()
        print("Function exists. Updating code")
        if update_config:
            sleep(5)  # avoid resource update conflict
        with open(_DEP_PACKAGE, "rb") as file:
            client.update_function_code(
                FunctionName=_get_function_name(),
                ZipFile=file.read(),
                Publish=True,
            )


def uninstall_function():
    _clear_scheduling()
    lambda_client = boto3.client("lambda")
    lambda_client.delete_function(FunctionName=_get_function_name())


def function_exists():
    client = boto3.client("lambda")
    try:
        client.get_function(FunctionName=_get_function_name())
        return True
    except ClientError as error:
        if error.response["Error"]["Code"] == "ResourceNotFoundException":
            return False
        else:
            raise error


def _get_function_name() -> str:
    return os.environ.get("FUNCTION_NAME", _DEFAULT_FUNCTION_NAME)


def _update_scheduling():
    _create_rule("collect-daily", "5 2-5 * * ? *", "collect", "day")
    _create_rule("detect-daily", "15 6-9 * * ? *", "detect", "day")
    _create_rule("aggregate-week", "15 6-9 ? * MON *", "aggregate", "week")
    _create_rule("detect-week", "20 8-11 ? * MON *", "detect", "week")
    _create_rule("aggregate-month", "20 5-6 1 * ? *", "aggregate", "month")
    _create_rule("detect-month", "20 7-11 1 * ? *", "detect", "month")
    _create_rule("clean-temp-table", "11 23 * * ? *", "clean-temp")


def _clear_scheduling():
    client = boto3.client("events")
    response = client.list_rule_names_by_target(TargetArn=_get_lambda_arn())
    rules = response['RuleNames']
    print(f"Going to delete rules: {rules}")
    for rule in rules:
        targets = client.list_targets_by_rule(Rule=rule)["Targets"]
        target_ids = [t["Id"] for t in targets]
        client.remove_targets(Rule=rule, Ids=target_ids)
        client.delete_rule(Name=rule, Force=True)
    if len(rules) > 0:
        lambda_client = boto3.client("lambda")
        function_name = _get_function_name()
        policy: str = lambda_client.get_policy(FunctionName=function_name)["Policy"]
        statement_ids = [row["Sid"] for row in json.loads(policy)["Statement"]]
        for sid in statement_ids:
            lambda_client.remove_permission(FunctionName=function_name, StatementId=sid)

