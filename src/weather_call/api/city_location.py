import requests
from weather_call.schema.city import CityData


def get_lat_long_from_api(city_name: str, country_code: str, api_key: str) -> dict:
    """
    Fetches latitude and longitude for a given city name using a geocoding API
    """

    city_response = requests.get(
        f"http://api.openweathermap.org/geo/1.0/direct?q={city_name},{country_code}&limit=1&appid={api_key}"
    )

    if city_response.status_code == 200:
        return city_response.json()

    else:
        raise Exception(
            f"Error fetching data from Geocoding API: {city_response.status_code}. Error getting city {city_name} from country {country_code}"
        )
