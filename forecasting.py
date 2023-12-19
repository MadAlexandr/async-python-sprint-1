#!/usr/bin/env python3
import logging
from multiprocessing import JoinableQueue
from pathlib import Path
from typing import NamedTuple, Iterable, Optional

import utils
from my_concurrent.process_pool import ProcessPool
from my_concurrent.queue_reader import QueueTaskReader
from my_concurrent.thread_fetcher import ThreadFetcher
from tasks import DataAggregationTask, TotalSummary
from tasks import DataAnalyzingTask
from tasks import DataCalculationTask
from tasks import DataFetchingTask
from tasks.data_calculation_task import CitySummary
from tasks.data_fetching_task import CityNameUrlPair, CityRawData


class Config(NamedTuple):
    cities_queue: JoinableQueue
    calculation_queue: JoinableQueue
    thread_fetcher: ThreadFetcher[CityNameUrlPair, CityRawData]
    process_pool: ProcessPool[CityRawData, CitySummary]
    aggregation_task: DataAggregationTask
    analyzing_task: DataAnalyzingTask


def configure(
        city_urls: Iterable[CityNameUrlPair],
        fetchers_count: int,
        calculation_workers_count: int
) -> Config:
    cities_queue: JoinableQueue = JoinableQueue()

    fetching_task = DataFetchingTask(city_urls)
    thread_fetcher = ThreadFetcher[CityNameUrlPair, CityRawData](
        fetching_task,
        fetchers_count,
        cities_queue
    )

    calculation_queue: JoinableQueue = JoinableQueue()
    process_pool = ProcessPool[CityRawData, CitySummary].make_pool(
        DataCalculationTask.calculate_summary_by_days,
        calculation_workers_count,
        cities_queue,
        calculation_queue
    )

    queue_reader = QueueTaskReader[CitySummary](calculation_queue)
    aggregation_task = DataAggregationTask(queue_reader)

    analyzing_task = DataAnalyzingTask()

    return Config(
        cities_queue,
        calculation_queue,
        thread_fetcher,
        process_pool,
        aggregation_task,
        analyzing_task,
    )


def find_bet_city(
        city_urls: Iterable[CityNameUrlPair],
        fetchers_count: int = 16,
        calculation_workers_count: int = 4,
        rating_file: Optional[Path] = None,
) -> list[TotalSummary]:
    config = configure(city_urls, fetchers_count, calculation_workers_count)

    config.process_pool.start_all()
    logging.info('start fetching cities data')
    config.thread_fetcher.fetch_data()
    config.cities_queue.join()
    logging.info('fetched all cities data')
    config.process_pool.stop_all()
    logging.info('calculate all cities summary by days')

    city_summaries = config.aggregation_task.aggregate_tasks()

    if rating_file:
        result = config.analyzing_task.calculate_ratings(
            city_summaries,
            rating_file
        )
    else:
        result = config.analyzing_task.find_best_city(
            city_summaries
        )

    return result


def print_cities(cities: list[TotalSummary]):
    for city in cities:
        print(
            f'{city.city}.'
            f' Shinny hours: {city.shiny_hours}.'
            f' Average temperature: {city.temp_avg:.2f}'
        )


if __name__ == '__main__':
    def main():
        import argparse

        arg_parser = argparse.ArgumentParser(
            prog='forecasting.py',
            description='Weather forecasts analyzer',
        )
        arg_parser.add_argument(
            '-f', '--fetchers',
            type=int,
            default=16,
            help='Number of data fetchers',
        )
        arg_parser.add_argument(
            '-w', '--workers',
            type=int,
            default=4,
            help='Number of analyzing workers',
        )
        arg_parser.add_argument(
            '-r', '--rating',
            action='store_true',
            help='Save cities rating in separate file'
        )
        arg_parser.add_argument(
            '-o',
            '--rating_file',
            type=Path,
            default='rating.csv',
            help='File to store cities rating'
        )
        args = arg_parser.parse_args()

        logging.basicConfig(level='INFO', filename='log.txt', filemode='w')
        print('Best city(ies):')
        print_cities(find_bet_city(
            utils.CITIES.items(),
            args.fetchers,
            args.workers,
            args.rating_file if args.rating else None
        ))

    main()
