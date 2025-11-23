from sqlalchemy import select
from sqlalchemy.orm import Session
import logging
import polars as pl
from datetime import datetime
from weather_call.model.weather import HourlyWeather
from weather_call.model.city import City

logger = logging.getLogger(__name__)


def distinct_weather(
    initial_time: datetime, final_time: datetime, session: Session
) -> pl.DataFrame:
    """Generate a report of distinct weather conditions between two timestamps."""
    stmt = select(HourlyWeather)

    df_full = pl.read_database(
        query=stmt,
        connection=session.connection(),
    )

    distinct_weather_df = (
        df_full.filter(pl.col("hourly_timestamp").is_between(initial_time, final_time))
        .select("weather_condition")
        .unique()
    )
    return distinct_weather_df


def rank_common_weather(
    initial_time: datetime, final_time: datetime, session: Session
) -> pl.DataFrame:
    """Generate a report of distinct weather conditions between two timestamps."""
    stmt = select(HourlyWeather).join(City).add_columns(City.name)

    df_full = pl.read_database(
        query=stmt,
        connection=session.connection(),
    )

    most_common_df = (
        df_full.filter(pl.col("hourly_timestamp").is_between(initial_time, final_time))
        .group_by(pl.col("name").alias("city"), "weather_condition")
        .agg(count=pl.count())
        .with_columns(
            pl.col("count").rank("dense", descending=True).over("city").alias("rank")
        )
        .filter(pl.col("rank") == 1)
        .drop("rank")
    )
    return most_common_df


def average_temperature(
    initial_time: datetime, final_time: datetime, session: Session
) -> pl.DataFrame:
    """Generate a report of distinct weather conditions between two timestamps."""
    stmt = select(HourlyWeather).join(City).add_columns(City.name)

    df_full = pl.read_database(
        query=stmt,
        connection=session.connection(),
    )

    avg_temp_df = (
        df_full.filter(pl.col("hourly_timestamp").is_between(initial_time, final_time))
        .group_by(pl.col("name").alias("city"))
        .agg(count=pl.mean("temperature"))
    )
    return avg_temp_df


def city_with_highest_column_value(
    column, initial_time: datetime, final_time: datetime, session: Session
) -> pl.DataFrame:
    """Generate a report of distinct weather conditions between two timestamps."""
    stmt = select(HourlyWeather).join(City).add_columns(City.name)

    df_full = pl.read_database(
        query=stmt,
        connection=session.connection(),
    )

    highest_attribute_df = (
        df_full.filter(pl.col("hourly_timestamp").is_between(initial_time, final_time))
        .with_columns(
            pl.col(column).rank("dense", descending=True).over("name").alias("rank")
        )
        .filter(pl.col("rank") == 1)
        .select(pl.col("name").alias("city"), pl.col(column))
    )
    return highest_attribute_df


def city_with_variation(
    initial_time: datetime, final_time: datetime, session: Session
) -> pl.DataFrame:
    """Generate a report of distinct weather conditions between two timestamps."""
    stmt = select(HourlyWeather).join(City).add_columns(City.name)

    df_full = pl.read_database(
        query=stmt,
        connection=session.connection(),
    )

    highest_temp_variation_df = (
        df_full.filter(pl.col("hourly_timestamp").is_between(initial_time, final_time))
        .with_columns(
            day=pl.col("hourly_timestamp").dt.day(),
        )
        .group_by(pl.col("name").alias("city"), "day")
        .agg(variation=pl.max("temperature") - pl.min("temperature"))
        .with_columns(
            pl.col("variation")
            .rank("dense", descending=True)
            .over("city")
            .alias("rank")
        )
        .filter(pl.col("rank") == 1)
        .drop("rank")
    )
    return highest_temp_variation_df
