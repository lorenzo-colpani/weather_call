from pydantic import BaseModel, Field, ConfigDict


class CityData(BaseModel):
    """
    Pydantic model to validate the structure of city data.
    """

    model_config = ConfigDict(
        populate_by_name=True,
        extra="ignore",
    )

    name: str = Field(alias="city_name", description="Name of the city")
    latitude: float = Field(alias="lat", description="Latitude of the city")
    longitude: float = Field(alias="lon", description="Longitude of the city")
    country_id: int = Field(description="Foreign key to the country table")


class CityDataBronze(BaseModel):
    """
    Pydantic model to validate the raw structure of city data from the API.
    """

    model_config = ConfigDict(
        populate_by_name=True,
        extra="ignore",
    )

    name: str = Field(description="Name of the city")
    country_id: int = Field(description="Foreign key to the country table")
    payload: dict = Field(description="Raw payload from the API")
