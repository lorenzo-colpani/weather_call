from sqlalchemy import ForeignKey, JSON, String, Numeric
from sqlalchemy.orm import relationship, Mapped, mapped_column
from weather_call.model.database import Base


class City(Base):
    """
    Static dimension table to store city location details (latitude/longitude).
    This table is pre-seeded and rarely changes.
    """

    # Foreign Key to DimCountry table
    country_id: Mapped[int] = mapped_column(ForeignKey("country.id"), index=True)

    # features
    name: Mapped[str] = mapped_column(String(100), index=True)

    latitude: Mapped[float] = mapped_column(Numeric(10, 6))
    longitude: Mapped[float] = mapped_column(Numeric(10, 6))

    # relationships
    country = relationship("Country", back_populates="city")
    hourly_weather = relationship("HourlyWeather", back_populates="city")
    hourly_weather_bronze = relationship("HourlyWeatherBronze", back_populates="city")

    def __repr__(self):
        return f"<City(city_name='{self.name}', lat={self.latitude}, lon={self.longitude})>"


class CityBronze(Base):
    """
    Bronze table to temporarily store raw city data ingested from external APIs.
    This table is used for initial data ingestion and may be cleaned or transformed
    before moving data to the DimCity table.
    """

    # foreign key to DimCountry table
    country_id: Mapped[int] = mapped_column(ForeignKey("country.id"), index=True)

    # features
    name: Mapped[str] = mapped_column(String(100), index=True)
    payload: Mapped[dict] = mapped_column(JSON)

    # flag to only read the last one added
    is_latest: Mapped[bool] = mapped_column(onupdate=False, default=True)

    # relationships
    country = relationship("Country", back_populates="city_bronze")

    def __repr__(self):
        return f"<CityBronze(city_name='{self.name}', payload={self.payload})>"
