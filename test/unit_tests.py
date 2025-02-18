import zipfile
from datetime import datetime

import pytest

from countdb_cli import _parse_cli_input
from pack_sources import zip_sources
from utils import days_range, weeks_range, month_range, get_last_finished_week, get_last_finished_month, get_day_back


class TestUtils:

    def test_days_range(self):
        days = list(days_range("2022-01-01", "2022-01-10"))
        assert len(days) == 10, days
        assert days[0] == "2022-01-01"
        assert days[1] == "2022-01-02"
        assert days[9] == "2022-01-10"

    def test_week_range(self):
        weeks = list(weeks_range("2022-07-01", "2022-07-30"))  # friday, saturday
        assert len(weeks) == 3, weeks
        assert weeks[0] == "2022-07-04"
        assert weeks[1] == "2022-07-11"
        assert weeks[2] == "2022-07-18"
        weeks = list(weeks_range("2022-07-04", "2022-07-11"))
        assert len(weeks) == 1, weeks
        assert weeks[0] == "2022-07-04"
        weeks = list(weeks_range("2022-08-01", "2022-08-07"))
        assert len(weeks) == 1, weeks
        assert weeks[0] == "2022-08-01"

    def test_month_range(self):
        months = list(month_range("2022-01-01", "2022-01-31"))
        assert len(months) == 1, months
        assert months[0] == "2022-01"
        months = list(month_range("2022-01-01", "2022-01-17"))
        assert len(months) == 0, months
        months = list(month_range("2022-01-10", "2022-03-11"))
        assert len(months) == 1, months
        assert months[0] == "2022-02"

    def test_last_finished_week(self):
        value = get_last_finished_week()
        assert len(value) == 10
        value_date = datetime.strptime(value, '%Y-%m-%d')
        assert value_date.weekday() == 0
        value = get_last_finished_week("2024-05-15")
        assert value == "2024-05-06"
        value = get_last_finished_week("2024-05-12")
        assert value == "2024-05-06"

    def test_last_finished_month(self):
        value = get_last_finished_month()
        assert len(value) == 7
        assert get_day_back(0)[:7] > value
        value = get_last_finished_month("2024-05-03")
        assert value == "2024-04"
        value = get_last_finished_month("2024-05-31")
        assert value == "2024-05"


class TestZipSources:
    def test_zip_sources(self):
        zip_file = zip_sources()
        assert zip_file.endswith(".zip")
        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            file_names = zip_ref.namelist()
            assert len(file_names) > 0
            assert "lambda_function.py" in file_names


class TestCLI:
    def test_no_such_operation(self, capsys):
        with pytest.raises(SystemExit):
            _parse_cli_input(["no_such_op"])
        assert "invalid choice: 'no_such_op" in capsys.readouterr().err
