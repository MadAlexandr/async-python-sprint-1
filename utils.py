import json


CITIES = json.load(open('cities.json'))

MIN_MAJOR_PYTHON_VER = 3
MIN_MINOR_PYTHON_VER = 9


def check_python_version():
    import sys

    if (
        sys.version_info.major < MIN_MAJOR_PYTHON_VER
        or sys.version_info.minor < MIN_MINOR_PYTHON_VER
    ):
        raise Exception(
            "Please use python version >= {}.{}".format(
                MIN_MAJOR_PYTHON_VER, MIN_MINOR_PYTHON_VER
            )
        )


def get_url_by_city_name(city_name):
    try:
        return CITIES[city_name]
    except KeyError:
        raise Exception("Please check that city {} exists".format(city_name))
