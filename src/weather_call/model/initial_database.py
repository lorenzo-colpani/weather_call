from sqlalchemy import select, Engine
from sqlalchemy.orm import Session
from weather_call.model.database import Base
from weather_call.model.city import City, CityBronze
from weather_call.model.country import Country
from weather_call.api.city_location import get_lat_long_from_api
from weather_call.schema.city import CityData, CityDataBronze
from weather_call.schema.country import CountryData
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
import logging

logger = logging.getLogger(__name__)


def create_db_and_tables(engine: Engine):
    """Initializes the database and creates all tables defined under Base."""
    logger.info("Creating database and tables if they do not exist.")
    Base.metadata.create_all(bind=engine)
    logger.info("Database and tables created successfully.")


def seed_initial_locations(session: Session, api_key: str):
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
        existing_city = session.execute(
            select(City).where(City.name == city_data["city_name"])
        ).scalar_one_or_none()

        if existing_city is not None:
            logger.info(
                f"City {city_data['city_name']} already exists in the database. Skipping insertion."
            )
            continue  # City already exists, skip to the next one

        country = session.execute(
            select(Country).where(Country.name == city_data["country_name"])
        ).one()
        logger.info(
            f"Found country in database: {city_data['country_name']} with ISO {country.iso_3166}"
        )

        # get lat long from api
        payload = get_lat_long_from_api(
            city_data["city_name"], country.iso_3166, api_key
        )
        logger.info(
            f"Retrieved lat/long from API for city: {city_data['city_name']} - Payload: {payload}"
        )

        # add the reponse to the bronze table first
        city_bronze = CityDataBronze(
            name=city_data["city_name"], country_id=country.id, payload=payload[0]
        )
        city_bronze_orm = CityBronze(**city_bronze.model_dump())
        logger.info(
            f"Adding city data to bronze table for city: {city_data['city_name']}"
        )

        session.add(city_bronze_orm)
        session.commit()

        city_pydantic = CityData(
            name=city_data["city_name"],
            latitude=city_bronze.payload["lat"],
            longitude=city_bronze.payload["lon"],
            country_id=country.id,
        )

        # Create a new ORM object and add it to the session
        city_orm = City(**city_pydantic.model_dump())
        session.add(city_orm)
        logger.info(
            f"Added city {city_data['city_name']} to session for insertion into city table."
        )

    # Commit changes to make them permanent in the database file
    session.commit()


def seed_initial_locations_countries(session: Session):
    """
    Populates the DimCityLocation table with the required cities
    if they do not already exist.
    """
    countries_list = [
        CountryData(country_name="italy", iso_3166="IT"),
    ]
    countries = [country.model_dump() for country in countries_list]
    stmt = sqlite_insert(Country).values(countries)

    stmt = stmt.on_conflict_do_nothing(
        index_elements=["name"]  # Which column checks for duplicates?
    )

    session.execute(stmt)
    session.commit()


def full_database_initialization(session: Session, engine: Engine, api_key: str):
    """
    Full database initialization including countries and cities.
    """
    create_db_and_tables(engine)
    seed_initial_locations_countries(session)
    seed_initial_locations(session, api_key)
