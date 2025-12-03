import datetime


class YearHalf:
    def __init__(self, year: int, half: int):
        self.year = year
        self.half = half

    def __str__(self):
        return f"{self.year}-H{self.half}"
    
    def __repr__(self):
        return f"YearHalf(year={self.year}, half={self.half})"

    def __eq__(self, other):
        if not isinstance(other, YearHalf):
            return False
        return self.year == other.year and self.half == other.half

    def __hash__(self):
        return hash((self.year, self.half))

    @staticmethod
    def from_date(date: datetime.date):
        return YearHalf(date.year, 1 if date.month <= 6 else 2)

    @staticmethod
    def from_str(string: str):
        try:
            year_part, half_part = string.split('-H')
            year = int(year_part)
            half = int(half_part)
            if half not in (1, 2):
                raise ValueError("El valor de 'half' debe ser 1 o 2.")
            return YearHalf(year, half)
        except Exception as e:
            raise ValueError(f"Formato inválido para YearHalf: '{string}'. Debe ser 'YYYY-H1' o 'YYYY-H2'.") from e

class MonthYear:
    def __init__(self, month: int, year: int):
        self.month = month
        self.year = year

    def __str__(self):
        return f"{self.year}-{self.month:02d}"

    @staticmethod
    def from_date(date: datetime.date):
        return MonthYear(date.month, date.year)

    @staticmethod
    def from_str(string: str):
        try:
            year_part, month_part = string.split('-')
            month = int(month_part)
            year = int(year_part)
            if month < 1 or month > 12:
                raise ValueError("El valor de 'month' debe estar entre 1 y 12.")
            return MonthYear(month, year)
        except Exception as e:
            raise ValueError(f"Formato inválido para MonthYear: '{string}'. Debe ser 'YYYY-MM'.") from e
