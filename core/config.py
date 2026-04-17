import os
from pydantic_settings import BaseSettings, SettingsConfigDict

# Dynamically resolve absolute path to the root .env file.
# This prevents pathing errors when running via PM2, systemd, or cron jobs.
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV_PATH = os.path.join(ROOT_DIR, ".env")

class Settings(BaseSettings):
    # Husky Pass Database Settings (casitaiedis)
    db_husky_host: str
    db_husky_port: int = 3306
    db_husky_user: str
    db_husky_password: str
    db_husky_name: str

    # Attendance Database Settings (control_coordinaciones)
    db_attendance_host: str
    db_attendance_port: int = 3306
    db_attendance_user: str
    db_attendance_password: str
    db_attendance_name: str

    # External APIs
    external_bot_api_url: str = "https://bot.casitaapps.com/fetch-base-simple"
    kardex_api_url: str = "https://kardex.casitaapps.com"

    # Automatically load from absolute .env file path
    model_config = SettingsConfigDict(env_file=ENV_PATH, env_file_encoding="utf-8", extra="ignore")

# Instantiate settings to be imported across the application
settings = Settings()