from weather_call.model.database import SessionLocal, engine
from weather_call.model.initial_database import full_database_initialization
from weather_call.config import Config
from weather_call.etl_service import add_new_hourly_data
from weather_call.reports import (
    distinct_weather,
    rank_common_weather,
    average_temperature,
    city_with_highest_column_value,
    city_with_variation,
)
import logging
import sys
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


# Configure the root logger
logging.basicConfig(
    level=logging.INFO,  # <--- This captures INFO, WARNING, ERROR
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)  # Prints to terminal
    ],
)

config = Config()
print(config.api_key)


def main():
    with SessionLocal() as session:
        # create all tables and initialize the database and tables
        full_database_initialization(session, engine, config.api_key)

        # add new hourly data to the weather table
        add_new_hourly_data(session, config.api_key)

        # reports request in order of the pdf
        initial_time = datetime.now() - timedelta(hours=24)
        final_time = datetime.now()
        # 1. distinct weather conditions in the last 24 hours
        distinct_df = distinct_weather(
            initial_time=initial_time, final_time=final_time, session=session
        )
        logger.info(f"Distinct weather conditions in the last 24 hours:\n{distinct_df}")

        # 2. rank the most common weather condition per city in the last 24 hours
        most_common_df = rank_common_weather(
            initial_time=initial_time, final_time=final_time, session=session
        )
        logger.info(
            f"Most common weather condition per city in the last 24 hours:\n{most_common_df}"
        )

        # 3. average temperature per city in the last 24 hours
        average_temp_df = average_temperature(
            initial_time=initial_time, final_time=final_time, session=session
        )
        logger.info(
            f"Average temperature per city in the last 24 hours:\n{average_temp_df}"
        )

        # 4. highest absolute temp city in the last 24 hours
        highest_temp_df = city_with_highest_column_value(
            "temperature", initial_time, final_time, session
        )
        logger.info(
            f"City with highest absolute temperature in the last 24 hours:\n{highest_temp_df}"
        )

        # 5. highest daily temperature variation city in the last 24 hours
        variation_temp_df = city_with_variation(initial_time, final_time, session)
        logger.info(
            f"City with highest daily temperature variation in the last 24 hours:\n{variation_temp_df}"
        )

        # 6. highest wind speed city in the last 24 hours
        highest_wind_df = city_with_highest_column_value(
            "wind_speed", initial_time, final_time, session
        )
        logger.info(
            f"City with highest wind speed in the last 24 hours:\n{highest_wind_df}"
        )


if __name__ == "__main__":
    main()
