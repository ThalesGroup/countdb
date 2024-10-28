import os
from datetime import datetime
from pathlib import Path
from time import sleep

import pytest

from deploy_lambda import deploy
from countdb_cli import init_evn_from_config_file


def _get_test_dir() -> str:
    return os.path.join(Path(os.path.dirname(os.path.abspath(__file__))).parent.absolute(), "test")


def _run_test_file(test_file: str, workers: int = 1,
                   log_durations: bool = False,
                   additional_log: bool = False) -> bool:
    args = []
    if additional_log:
        args.append("--log-cli-level=INFO")
    if log_durations:
        args.append("--durations=0")
    args.append(os.path.join(_get_test_dir(), test_file))
    if workers > 1:
        args.append(f"--dist=loadgroup")
        args.append(f"-n {workers}")

    return pytest.main(args) == 0


def run_tests(unit_tests: bool = True, integration_tests: bool = True) -> bool:
    if unit_tests:
        if not _run_test_file("unit_tests.py"):
            return False
        if not _run_test_file("unit_aws_mock_tests.py", workers=3, additional_log=True):
            return False
    if integration_tests:
        if not _run_test_file("integration_tests.py", workers=1, log_durations=True, additional_log=True):
            return False
        init_evn_from_config_file()
        deploy(False, True)
        sleep(5)
        if not _run_test_file("lambda_integration_tests.py", workers=1, log_durations=True, additional_log=True):
            return False

    return True


if __name__ == '__main__':
    start_time = datetime.now()
    run_unit = True if "SKIP_UNIT" not in os.environ else False
    run_integration = True if "SKIP_INTEGRATION" not in os.environ else False

    print(f"START TESTING. Unit: {run_unit}, Integration: {run_integration}")
    print("Note: Use pip install pytest-xdist to allow parallel test runs")
    result = run_tests(unit_tests=run_unit, integration_tests=run_integration)
    print(f"Finished running tests. Unit: {run_unit}, Integration: {run_integration}. "
          f"Elapsed time: {datetime.now() - start_time}. Result: {result}")
    exit(0 if result else 1)
