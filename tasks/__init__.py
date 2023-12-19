import logging

from .data_fetching_task import DataFetchingTask
from .data_calculation_task import DataCalculationTask
from .data_aggregation_task import DataAggregationTask, TotalSummary
from .data_analyzing_task import DataAnalyzingTask

__all__ = [
    'DataFetchingTask',
    'DataCalculationTask',
    'DataAggregationTask',
    'TotalSummary',
    'DataAnalyzingTask',
]

logger = logging.getLogger('forecasting')
logger.setLevel(logging.INFO)

file_handler = logging.FileHandler('forecasting.log')
file_handler.setLevel(logging.DEBUG)
logger.addHandler(file_handler)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
logger.addHandler(console_handler)
