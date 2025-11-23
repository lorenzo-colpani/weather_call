import requests


def get_weather(lat: float, long: float, api_key: str) -> dict:
    """
    Fetches latitude and longitude for a given city name using a geocoding API
    """
    part = "current,minutely,daily,alerts"
    units = "metric"
    response = requests.get(
        f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={long}&exclude={part}&units={units}&appid={api_key}"
    )

    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(
            f"Error fetching data from Hourly Weather API: {response.status_code}. Error getting weather for lat {lat} and long {long}"
        )
