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
        .agg(frequency=pl.count())
        .with_columns(
            pl.col("frequency")
            .rank("dense", descending=True)
            .over("city")
            .alias("rank")
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
        .agg(average_temperature=pl.mean("temperature"))
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
        .top_k(1, by=pl.col(column).abs())
        .select(pl.col("name").alias("city"), column)
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
            day=pl.col("hourly_timestamp").dt.date(),
        )
        .group_by(pl.col("name").alias("city"), "day")
        .agg(variation=pl.max("temperature") - pl.min("temperature"))
        .top_k(1, by=pl.col("variation"))
    )
    return highest_temp_variation_df
