from sqlalchemy import String, CHAR
from sqlalchemy.orm import relationship, mapped_column, Mapped
from weather_call.model.database import Base


class Country(Base):
    """country table definition"""

    iso_3166: Mapped[str] = mapped_column(CHAR(2), unique=True)
    name: Mapped[str] = mapped_column(String(100), unique=True, index=True)

    # relationships
    city = relationship("City", back_populates="country")
    city_bronze = relationship("CityBronze", back_populates="country")

    def __repr__(self):
        return f"<Country(country_name='{self.name}', iso_3166='{self.iso_3166}')>"
