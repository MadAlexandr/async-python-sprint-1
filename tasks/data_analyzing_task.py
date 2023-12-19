import logging
from pathlib import Path
from typing import Iterable, Any

from common_algorithms.rank import get_rank
from tasks.data_aggregation_task import TotalSummary, Rating
from tasks.data_calculation_task import DaysSummary

logger = logging.getLogger('forecasting')


class DataAnalyzingTask:
    @staticmethod
    def find_best_city(
            cities_data: Iterable[TotalSummary]
    ) -> list[TotalSummary]:
        max_rating = Rating(0, -271)
        best = []
        for cd in cities_data:
            cd_rating = cd.rating
            if max_rating < cd_rating:
                max_rating = cd_rating
                best = [cd]
            elif max_rating == cd_rating:
                best.append(cd)

        return best

    @staticmethod
    def calculate_ratings(
            cities_data: Iterable[TotalSummary],
            rating_file: Path
    ):
        cities_data = list(cities_data)
        sorted_cities = sorted(
            cities_data,
            key=lambda cd: cd.rating,
            reverse=True
        )
        header, dates = DataAnalyzingTask._make_header(sorted_cities)
        rows = [header]
        ratings = [c.rating for c in sorted_cities]

        for city, rank in zip(sorted_cities, get_rank(ratings)):
            row_one: list[Any] = [city.city_summary.city, 'temperature']
            row_one.extend(DataAnalyzingTask._extract_day_measurement(
                city.city_summary.days_summary,
                dates,
                'temp_avg'
            ))
            row_one.append(city.temp_avg)
            row_one.append(rank)
            row_two: list[Any] = ['', 'shiny hours']
            row_two.extend(DataAnalyzingTask._extract_day_measurement(
                city.city_summary.days_summary,
                dates,
                'relevant_cond_hours'
            ))
            row_two.append(city.shiny_hours_avg)
            row_two.append('')
            rows.append(row_one)
            rows.append(row_two)

        rating_file.write_text('\n'.join(
            ','.join(map(str, row))
            for row in rows
        ))

        logger.info(f'Rating saved in {rating_file}')

        return DataAnalyzingTask.find_best_city(cities_data)

    @staticmethod
    def _make_header(
            cities_data: list[TotalSummary]
    ) -> tuple[list[str], list[str]]:
        header = ['City', 'Measurement/days']
        all_dates = {
            d['date']
            for ts in cities_data
            for d in ts.city_summary.days_summary['days']
        }
        dates = sorted(all_dates)
        header.extend(dates)
        header.extend(['Average', 'Rating'])

        return header, dates

    @staticmethod
    def _extract_day_measurement(
            days_summary: DaysSummary,
            dates: list[str],
            key: str
    ) -> list[int | float | str]:

        result = []

        for date in dates:
            for day in days_summary['days']:
                if day['date'] == date:
                    if key == 'temp_avg':
                        m = day['temp_avg']
                    elif key == 'relevant_cond_hours':
                        m = day['relevant_cond_hours']
                    else:
                        m = None
                    result.append(m if m is not None else '')
                    break
            else:
                result.append('')

        return result
