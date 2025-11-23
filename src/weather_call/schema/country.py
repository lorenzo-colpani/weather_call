from sqlalchemy.sql.base import _DefaultDescriptionTuple
from pydantic import BaseModel, ConfigDict, Field, field_validator


class CountryData(BaseModel):
    """
    Pydantic model to validate the structure of country data.
    """

    model_config = ConfigDict(
        populate_by_name=True,
        extra="ignore",
    )

    name: str = Field(alias="country_name", description="Name of the country")
    iso_3166: str = Field(
        min_length=2, max_length=2, description="ISO 3166-1 alpha-2 country code"
    )

    @field_validator("iso_3166")
    @classmethod
    def ensure_uppercase(cls, v: str) -> str:
        """Ensure the ISO country code is uppercase."""
        return v.upper()
