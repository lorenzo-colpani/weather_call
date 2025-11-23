from pydantic_settings import BaseSettings, SettingsConfigDict


# 1. Define the Pydantic Model
class Config(BaseSettings):
    """keep api key and other config variables from env file"""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        populate_by_name=True,
        extra="ignore",
    )

    api_key: str
