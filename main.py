from weather_call.model.database import SessionLocal, engine
from weather_call.model.initial_database import full_database_initialization
from weather_call.config import Config

config = Config()


def main():
    with SessionLocal() as session:
        full_database_initialization(session, engine, config.api_key)


if __name__ == "__main__":
    main()
