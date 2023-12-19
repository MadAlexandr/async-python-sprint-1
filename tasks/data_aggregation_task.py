import logging
from dataclasses import dataclass
from functools import reduce
from typing import NamedTuple, Optional, Protocol, TypeVar, Iterable

from common_types.task_types import TaskState, Status
from tasks.data_calculation_task import CitySummary, DaySummary


logger = logging.getLogger('forecasting')


class Rating(NamedTuple):
    good_hours: int
    temp_avg: float


@dataclass
class TotalSummary:
    city_summary: CitySummary
    temp_sum: float
    shiny_hours: int
    analyzing_hours: int
    analyzing_days: int

    @property
    def city(self):
        return self.city_summary.city

    @property
    def temp_avg(self) -> float:
        if self.analyzing_hours == 0:
            return 0

        return round(self.temp_sum / self.analyzing_hours, 1)

    @property
    def shiny_hours_avg(self) -> float:
        return round(self.shiny_hours / self.analyzing_days, 1)

    @property
    def rating(self) -> Rating:
        return Rating(self.shiny_hours, self.temp_avg)


TData = TypeVar('TData')


class TaskReader(Protocol[TData]):
    def read_tasks(self) -> Iterable[TaskState[TData]]:
        ...


class DataAggregationTask:
    def __init__(self, task_reader: TaskReader[CitySummary]):
        self._task_reader = task_reader

    def aggregate_tasks(self) -> Iterable[TotalSummary]:
        for task in self._task_reader.read_tasks():
            if task.status == Status.ERROR:
                self._on_error(task.message)
                continue

            if task.data is None:
                continue

            ts = self.aggregate_city_summary(task.data)
            if ts is None:
                continue

            yield ts

    def _on_error(self, message: Optional[str]):
        logger.warning(f'Trying get city summary failed: "{message}"')

    def aggregate_city_summary(
            self,
            city_summary: CitySummary
    ) -> Optional[TotalSummary]:
        try:
            days_summary = city_summary.days_summary
            fulfilled_days = filter(self._filter_day, days_summary['days'])
        except KeyError:
            logger.error('Incorrect city summary format')
            return None
        else:
            return reduce(
                self._aggregate_day,
                fulfilled_days,
                TotalSummary(city_summary, 0, 0, 0, 0)
            )

    def _filter_day(self, day_data: DaySummary):
        return (day_data['hours_count'] != 0
                and day_data['temp_avg'] is not None
                and 'relevant_cond_hours' in day_data)

    def _aggregate_day(
            self,
            total_summary: TotalSummary,
            day_data: DaySummary
    ) -> TotalSummary:
        hours = day_data['hours_count']
        total_summary.temp_sum += day_data['temp_avg'] * hours
        total_summary.shiny_hours += day_data['relevant_cond_hours']
        total_summary.analyzing_hours += hours
        total_summary.analyzing_days += 1

        return total_summary
