from sqlalchemy import select, Engine
from sqlalchemy.orm import Session
from weather_call.model.database import Base
from weather_call.model.city import City
from weather_call.model.country import Country
from weather_call.schema.weather import HourlyWeatherData, HourlyWeatherDataBronze
from weather_call.model.weather import HourlyWeatherBronze, HourlyWeather
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from weather_call.api.hour_weather import get_weather
from datetime import datetime, timezone, timedelta
import logging

logger = logging.getLogger(__name__)


def add_new_hourly_data(session: Session, api_key: str):
    """
    Populates the DimCityLocation table with the required cities
    if they do not already exist.
    """
    cities = [
        {"city_name": "milan", "country_name": "italy"},
        {"city_name": "bologna", "country_name": "italy"},
        {"city_name": "cagliari", "country_name": "italy"},
    ]
    for city_data in cities:
        logger.info(
            f"Processing city: {city_data['city_name']}, {city_data['country_name']}"
        )
        # Check if the city already exists to prevent duplicate insertions
        city_value = session.execute(
            select(City.latitude, City.longitude, City.id)
            .join(Country)
            .where(
                (City.name == city_data["city_name"])
                & (Country.name == city_data["country_name"])
            )
        ).one()
        logger.debug(f"City value: {city_value}")

        logger.info(
            f"Found city in database: {city_data['city_name']} at lat {city_value.latitude}, long {city_value.longitude}"
        )

        # get lat long from api
        payload = get_weather(
            lat=city_value.latitude, long=city_value.longitude, api_key=api_key
        )
        dt = payload["dt"]
        full_timestamp = datetime.fromtimestamp(dt, timezone.utc)
        hourly_timestamp = full_timestamp.replace(minute=0, second=0, microsecond=0)

        # add the reponse to the bronze table first
        hourly_bronze = HourlyWeatherDataBronze(
            city_id=city_value.id, payload=payload, hourly_timestamp=hourly_timestamp
        )
        logger.info(
            f"Adding hourly weather data to bronze table for city: {city_data['city_name']}"
        )
        logger.debug(f"Payload: {hourly_bronze.payload}")
        city_bronze_orm = HourlyWeatherBronze(**hourly_bronze.model_dump())

        session.add(city_bronze_orm)
        session.commit()

        cutoff_time = datetime.now(timezone.utc) - timedelta(days=3)
        if hourly_timestamp < cutoff_time:
            logger.info(
                f"Skipping processing for old data at {hourly_timestamp} for city: {city_data['city_name']}"
            )
            continue

        hourly_pydantic = HourlyWeatherData(
            city_id=city_value.id,
            temperature=payload["main"]["temp"],
            wind_speed=payload["wind"]["speed"],
            weather_condition=payload["weather"][0]["main"],
            hourly_timestamp=hourly_timestamp,
        )

        # Create a new ORM object and add it to the session
        hourly_weather_stmt = sqlite_insert(HourlyWeather).values(
            hourly_pydantic.model_dump()
        )
        hourly_weather_stmt = hourly_weather_stmt.on_conflict_do_update(
            index_elements=["city_id", "hourly_timestamp"],
            set_={
                "temperature": hourly_weather_stmt.excluded.temperature,
                "wind_speed": hourly_weather_stmt.excluded.wind_speed,
                "weather_condition": hourly_weather_stmt.excluded.weather_condition,
                "updated_at": datetime.now(timezone.utc),
            },
        )
        session.execute(hourly_weather_stmt)

    # Commit changes to make them permanent in the database file
    session.commit()
