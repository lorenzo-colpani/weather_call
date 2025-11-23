from pydantic import BaseModel, Field, ConfigDict
from datetime import datetime


class HourlyWeatherData(BaseModel):
    """
    Pydantic model to validate the structure of city data.
    """

    model_config = ConfigDict(
        populate_by_name=True,
        extra="ignore",
    )

    city_id: int = Field(description="Foreign key to the city table")

    hourly_timestamp: datetime = Field(description="datetime")
    temperature: float = Field(description="temperature in celsius")
    wind_speed: float = Field(description="wind speed in meter/sec")
    weather_condition: str = Field(description="weather condition description")


class HourlyWeatherDataBronze(BaseModel):
    """
    Pydantic model to validate the raw structure of hourly weather data from the API.
    """

    model_config = ConfigDict(
        populate_by_name=True,
        extra="ignore",
    )
    hourly_timestamp: datetime = Field(description="datetime")
    city_id: int = Field(description="Foreign key to the city table")
    payload: dict = Field(description="Raw payload from the API")
