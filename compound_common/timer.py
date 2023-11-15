import datetime
from dataclasses import dataclass
from datetime import timedelta
from typing import Union


@dataclass
class Timer:
    start: datetime.datetime
    end: Union[datetime.datetime, None]

    def delta(self) -> timedelta:
        result = self.end - self.start
        return result
