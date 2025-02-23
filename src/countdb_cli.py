import argparse
import base64
import json
import os
import re
import sys
from base64 import b64decode
from typing import List

import boto3
import botocore.config
from botocore.exceptions import ClientError, NoCredentialsError

_DEFAULT_CONFIG_FILE = "countdb.config.json"


def _parse_admin_cli_input(argv) -> dict:
    parser = argparse.ArgumentParser("countdb")
    parser.add_argument("operation",
                        choices=["install", "uninstall"],
                        help="Operation")
    parser.add_argument("--version", required=False, default="latest",
                        help="By default latest version is installed. It is possible to choose a version from github releases using the 'sources' keyword")
    parser.add_argument("--config", required=False, default=_DEFAULT_CONFIG_FILE,
                        help=f"Configuration file. Default file is {_DEFAULT_CONFIG_FILE}")
    parser.add_argument("--verbose", required=False,
                        help="Verbose output", action=argparse.BooleanOptionalAction, default=False)
    args_namespace = parser.parse_args(args=argv)
    args = {k: v for k, v in vars(args_namespace).items() if v}
    return args


def _parse_cli_input(argv) -> dict:
    parser = argparse.ArgumentParser("countdb")
    parser.add_argument("operation",
                        choices=["upload", "collect", "aggregate", "detect", "clear", "init"],
                        help="Operation")
    parser.add_argument("-d", "--dataset", required=False,
                        help="For upload dataset file name. For other operations dataset name. "
                             "If not send all datasets are used")
    parser.add_argument("--day", required=False, help="Single day. If no day is sent last ended day is used")
    parser.add_argument("--from", required=False, dest="from_day", help="From day")
    parser.add_argument("--to", required=False, dest="to_day", help="To day")
    parser.add_argument("--counter", required=False, help="Dataset Counter")
    parser.add_argument("--interval", required=False,
                        choices=["day", "week", "month"], help="Aggregate or Detect interval")
    parser.add_argument("--override", required=False, help="Override existing data",
                        action='store_true')
    parser.add_argument("--table", required=False, help="Init or Clear for a specific table")
    parser.add_argument("--config", required=False, default=_DEFAULT_CONFIG_FILE,
                        help=f"Configuration file. Default file is {_DEFAULT_CONFIG_FILE}")
    parser.add_argument("--verbose", required=False,
                        help="Verbose output", action=argparse.BooleanOptionalAction, default=False)
    args_namespace = parser.parse_args(args=argv)
    args = {k: v for k, v in vars(args_namespace).items() if v}
    return args


def _run_cli_command(command: dict):
    print(f"Function: {_get_function_name()}. Going to run command: {command}")
    operation = command["operation"]
    verbose = command.get("verbose", False)
    if operation == "install":
        result = _install(command["version"], verbose)
    elif operation == "uninstall":
        result = _uninstall(verbose)
    elif operation == "upload":
        result = _upload_dataset(command["dataset"], verbose)
    else:
        result = _run_operation(command, verbose)
    print(result)


def _get_function_name() -> str:
    return os.environ["FUNCTION_NAME"] if "FUNCTION_NAME" in os.environ else "countdb"


def _invoke(payload, verbose: bool):
    lambda_client = boto3.client("lambda", config=botocore.config.Config(read_timeout=900))
    response = lambda_client.invoke(
        FunctionName=_get_function_name(),
        Payload=payload, LogType="Tail")
    if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
        print(f"Error response from lambda: {response}", response)
    if verbose:
        print(f"{'=' * 40}LOGS{'=' * 40}\n{b64decode(response['LogResult']).decode('UTF8')}")


def _upload_dataset(path: str, verbose: bool):
    with open(path) as f:
        b64_conf = base64.b64encode(f.read().encode("utf-8"))
    return _run_operation({"operation": "upload", "data": b64_conf.decode()}, verbose)


def _run_operation(payload, verbose: bool):
    if _local_mode():
        from lambda_function import lambda_handler
        return lambda_handler(payload, None)
    else:
        try:
            return _invoke(bytes(json.dumps(payload), encoding="UTF8"), verbose)
        except ClientError as e:
            if "Function not found" in e.response["Message"]:
                return f"Function not found: {_get_function_name()}"


def _install(version: str, verbose: bool):
    try:
        from deploy_lambda import deploy, function_exists
        if function_exists():
            deploy(version, update_config=True, update_code=True)
        else:
            deploy(version)
            print("Creating database")
            _run_operation({"operation": "init"}, verbose)
        return {"success": True, "version": version}
    except Exception as e:
        return {"success": False, "error": str(e)}


def _uninstall(verbose: bool):
    from deploy_lambda import uninstall_function, function_exists
    try:
        if function_exists():
            _run_operation({"operation": "drop-database"}, verbose)
            uninstall_function()
            return {"success": True}
        else:
            return {"success": False, "error": "Function not found"}
    except Exception as e:
        return {"success": False, "error": str(e)}

def _init_env(config_file: str) -> bool:
    init_evn_from_config_file(config_file)
    creds_str = os.environ.get("CREDS")
    if creds_str is not None:
        creds_str = creds_str.replace("\"", "")
        m = re.search("accessKey: ([^,]+),", creds_str, flags=re.MULTILINE)
        if m:
            os.environ["AWS_ACCESS_KEY_ID"] = m.groups()[0]
        m = re.search("secretKey: ([^,]+),", creds_str, flags=re.MULTILINE)
        if m:
            os.environ["AWS_SECRET_ACCESS_KEY"] = m.groups()[0]
        m = re.search("securityToken: ([^,]+),", creds_str, flags=re.MULTILINE)
        if m:
            os.environ["AWS_SECURITY_TOKEN"] = m.groups()[0]
    else:
        creds_file = os.path.join(os.getcwd(), "aws", "config")
        if os.path.exists(creds_file):
            print(f"Using creds file: {creds_file}")
            os.environ["AWS_CONFIG_FILE"] = creds_file


    sts = boto3.client('sts')
    try:
        result = sts.get_caller_identity()
        print(f"Credentials are valid. Account: {result['Account']}")
        return True
    except NoCredentialsError:
        print("Unable to locate credentials")
    except ClientError as e:
        print(f"Credentials are not valid:\n{str(e)}")
    return False


def init_evn_from_config_file(config_file: str = _DEFAULT_CONFIG_FILE):
    if os.path.exists(config_file):
        print(f"Using config file: {config_file}")
        with open(config_file) as f:
            config_dict = json.load(f)
        for k in config_dict:
            if k not in os.environ:
                os.environ[k.upper().replace("-", "_")] = config_dict[k]
        if "region" in config_dict:
            os.environ["AWS_DEFAULT_REGION"] = config_dict["region"]

def _local_mode():
    return os.environ.get("LOCAL_MODE", "false").lower() == "true"


def run_cli(cli_args: List[str]):
    is_admin = len(cli_args) > 1 and cli_args[1] == "admin"
    if is_admin:
        parsed_command = _parse_admin_cli_input(cli_args[2:])
    else:
        parsed_command = _parse_cli_input(cli_args[1:])
    if _init_env(parsed_command["config"]):
        del parsed_command["config"]
        _run_cli_command(parsed_command)


if __name__ == '__main__':
    run_cli(sys.argv)
