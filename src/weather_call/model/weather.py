from sqlalchemy import ForeignKey, JSON, String, Numeric, UniqueConstraint
from sqlalchemy.orm import relationship, Mapped, mapped_column
from weather_call.model.database import Base
from datetime import datetime


class HourlyWeather(Base):
    """
    Static dimension table to store city location details (latitude/longitude).
    This table is pre-seeded and rarely changes.
    """

    # Foreign Key to City table
    city_id: Mapped[int] = mapped_column(ForeignKey("city.id"), index=True)

    # features
    hourly_timestamp: Mapped[datetime] = mapped_column(index=True)
    temperature: Mapped[float] = mapped_column(Numeric(5, 2))
    wind_speed: Mapped[float] = mapped_column(Numeric(5, 2))
    weather_condition: Mapped[str] = mapped_column(String(100))

    # relationships
    city = relationship("City", back_populates="hourly_weather")
    __table_args__ = (
        UniqueConstraint("city_id", "hourly_timestamp", name="uq_city_time"),
    )

    def __repr__(self):
        return f"<HourlyWeather(city_id='{self.city_id}', hourly_timestamp={self.hourly_timestamp}, temperature={self.temperature}, wind_speed={self.wind_speed}, weather_condition='{self.weather_condition}')>"


class HourlyWeatherBronze(Base):
    """
    Bronze table to temporarily store raw city data ingested from external APIs.
    This table is used for initial data ingestion and may be cleaned or transformed
    before moving data to the DimCity table.
    """

    # foreign key to City table
    city_id: Mapped[int] = mapped_column(ForeignKey("city.id"), index=True)

    # features
    hourly_timestamp: Mapped[datetime] = mapped_column(index=True)
    payload: Mapped[dict] = mapped_column(JSON)

    # flag to only read the last one added
    is_latest: Mapped[bool] = mapped_column(onupdate=False, default=True)

    # relationships
    city = relationship("City", back_populates="hourly_weather_bronze")

    def __repr__(self):
        return f"<CityBronze(hourly_timestamp='{self.hourly_timestamp}', payload={self.payload})>"
