from typing import TypeAlias, NamedTuple, Iterable

from external.client import YandexWeatherAPI


CityName: TypeAlias = str
CityUrl: TypeAlias = str
CityNameUrlPair: TypeAlias = tuple[CityName, CityUrl]


class CityRawData(NamedTuple):
    city: CityName
    data: dict


class DataFetchingTask:
    def __init__(self, sources: Iterable[CityNameUrlPair]):
        self._sources = sources

    def get_sources(self) -> Iterable[CityNameUrlPair]:
        return self._sources

    def fetch_source(self, city_source: CityNameUrlPair) -> CityRawData:
        city_name, city_url = city_source

        return CityRawData(
            city=city_name,
            data=YandexWeatherAPI.get_forecasting(city_url),
        )
