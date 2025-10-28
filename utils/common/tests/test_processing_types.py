import datetime

from utils.common.processing_types import YearHalf, MonthYear


def test_year_half_from_date():
    date = datetime.date(2023, 5, 10)
    yh = YearHalf.from_date(date)
    assert str(yh) == "2023-H1"

def test_month_year_from_date():
    date = datetime.date(2023, 11, 2)
    my = MonthYear.from_date(date)
    assert str(my) == "2023-11"

def test_month_year_from_date_january():
    date = datetime.date(2023, 1, 15)
    my = MonthYear.from_date(date)
    assert str(my) == "2023-01"