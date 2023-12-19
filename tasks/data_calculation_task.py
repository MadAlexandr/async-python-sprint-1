from typing import TypedDict, NamedTuple

from external.analyzer import analyze_json
from tasks.data_fetching_task import CityRawData


class DaySummary(TypedDict):
    date: str
    hours_start: int
    hours_end: int
    hours_count: int
    temp_avg: float | int
    relevant_cond_hours: int


class DaysSummary(TypedDict):
    days: list[DaySummary]


class CitySummary(NamedTuple):
    city: str
    days_summary: DaysSummary


class DataCalculationTask:
    @staticmethod
    def calculate_summary_by_days(raw_city_data: CityRawData) -> CitySummary:
        city = raw_city_data.city
        days_summary = analyze_json(raw_city_data.data)

        return CitySummary(
            city=city,
            days_summary=days_summary,
        )
