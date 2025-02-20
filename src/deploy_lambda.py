import http.client
import json
import os
import tempfile
from random import randint
from time import sleep
from typing import List

import boto3
from botocore.exceptions import ClientError

_REQUIRED_ENV_VARS = ["BUCKET", "WORKGROUP", "ATHENA_LOGS"]
_OPTIONAL_ENV_VARS = ["ROOT_FOLDER", "DATABASE_NAME", "TEMP_DATABASE_NAME"]
_DEFAULT_FUNCTION_NAME = "countdb"

_GITHUB_REPO_OWNER = "ThalesGroup"
_GITHUB_REPO_NAME = "countdb"



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


def deploy(version: str, update_config: bool = False, update_code: bool = True):
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
        dep_package = _get_package(version)
        with open(dep_package, "rb") as file:
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
        dep_pacakge = _get_package(version)
        print("Function exists. Updating code")
        if update_config:
            sleep(5)  # avoid resource update conflict
        with open(dep_pacakge, "rb") as file:
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

def _get_package(pacakge_version: str) -> str:
    if pacakge_version == "sources":
        from pack_sources import zip_sources
        return zip_sources()
    elif pacakge_version == "latest":
        return _download_sources("latest")
    else:
        versions = _get_versions()
        if pacakge_version not in versions:
            raise ValueError(f"Unknown version: {pacakge_version}")
        return _download_sources(pacakge_version)


def _github_api_request(url: str = None) -> dict:
    conn = None
    try:
        conn = http.client.HTTPSConnection("api.github.com")
        headers = {
            "User-Agent": "Python http.client",
            "Accept": "application/vnd.github.v3+json"
        }
        if "GITHUB_TOKEN" in os.environ:
            headers["Authorization"] = f"token {os.environ['GITHUB_TOKEN']}"
        final_url = f"/repos/{_GITHUB_REPO_OWNER}/{_GITHUB_REPO_NAME}"
        if url:
            final_url += f"/{url}"
        conn.request("GET", final_url, headers=headers)
        response = conn.getresponse()
        if response.status == 404:
            raise ValueError("Repository not found or you do not have access.")
        elif response.status == 403:
            if response.getheader('X-RateLimit-Remaining') == '0':
                raise ValueError("API rate limit exceeded.")
            else:
                raise ValueError("Access forbidden. Check your token permissions or repository access.")
        elif response.status != 200:
            raise ValueError(f"Unknown error: {response.read().decode()}")
        else:
            return json.loads(response.read().decode())
    finally:
        if conn:
            conn.close()

def _get_versions() -> List[str]:
    releases = _github_api_request("releases")
    return [r["tag_name"] for r in releases]

def _download_sources(version: str):
    if version == "latest":
        release = _github_api_request(f"releases/latest")
    else:
        release = _github_api_request(f"releases/tags/{version}")
    asset = release["assets"][0]

    conn = None
    try:
        conn = http.client.HTTPSConnection("api.github.com")
        headers = {
            "User-Agent": "Python http.client",
            "Accept": "application/octet-stream"
        }
        if "GITHUB_TOKEN" in os.environ:
            headers["Authorization"] = f"token {os.environ['GITHUB_TOKEN']}"
        conn.request("GET", f"/repos/{_GITHUB_REPO_OWNER}/{_GITHUB_REPO_NAME}/releases/assets/{asset['id']}", headers=headers)
        response = conn.getresponse()
        if response.status == 302:
            download_url = response.getheader("Location")
            conn.close()
            conn = http.client.HTTPSConnection(download_url.split('/')[2])
            conn.request("GET", download_url.split(download_url.split('/')[2])[1], headers=headers)
            response = conn.getresponse()
            file_name = os.path.join(tempfile.gettempdir(), asset["name"])
            with open(file_name, "wb") as file:
                file.write(response.read())
            return file_name
        else:
            raise ValueError(f"Failed to download asset: {response.status}")
    finally:
        if conn:
            conn.close()
