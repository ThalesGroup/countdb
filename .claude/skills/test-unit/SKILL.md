---
name: test-unit
description: Run all unit tests for countdb (plain unit tests and moto-based AWS mock tests) using test/run_tests.py
disable-model-invocation: true
allowed-tools: Bash(SKIP_INTEGRATION=1 PYTHONPATH=src:test venv/bin/python test/run_tests.py*)
---

# Unit Tests

Run all unit tests for the countdb project. No AWS credentials are needed:
AWS calls are mocked with moto.

## Usage

```
/test-unit
```

## What it does

1. Runs `test/unit_tests.py`
2. Runs `test/unit_aws_mock_tests.py` (moto mocks, 3 parallel workers via pytest-xdist)
3. Skips the integration tests (`SKIP_INTEGRATION=1`), which need real AWS access
4. Sets `PYTHONPATH` to include the `src` and `test` directories

## Prerequisites

Ensure dependencies are installed:
```sh
python -m venv venv && source venv/bin/activate && pip install --upgrade pip && pip install -r requirements.txt && pip install -r test/requirements.txt
```

## Command

```sh
SKIP_INTEGRATION=1 PYTHONPATH=src:test venv/bin/python test/run_tests.py
```

The last line of the output is `Finished running tests ... Result: True|False`
(the exit code is 0 on success).

## Integration tests

Not part of this skill. They run against a real AWS account and deploy the Lambda,
so run them explicitly with the test profile:

```sh
SKIP_UNIT=1 AWS_PROFILE=test PYTHONPATH=src:test venv/bin/python test/run_tests.py
```
