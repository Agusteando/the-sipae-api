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

    # SIPAE App Database Settings (dirs / coords / team_members / health report logs)
    db_sipae_host: str = ""
    db_sipae_port: int = 3306
    db_sipae_user: str = ""
    db_sipae_password: str = ""
    db_sipae_name: str = ""

    # Health Reports / Gmail Automation
    health_reports_enabled: bool = False
    health_reports_timezone: str = "America/Mexico_City"
    health_reports_send_hour: int = 15
    health_reports_send_minute: int = 55
    health_reports_admin_token: str = ""
    health_reports_public_base_url: str = ""
    health_reports_test_recipient: str = ""
    health_reports_gmail_sender: str = ""
    google_service_account_email: str = ""
    google_private_key: str = ""
    google_workspace_domain: str = "casitaiedis.edu.mx"

    # External APIs
    external_bot_api_url: str = "https://bot.casitaapps.com/fetch-base-simple"
    kardex_api_url: str = "https://kardex.casitaapps.com"

    # Automatically load from absolute .env file path
    model_config = SettingsConfigDict(env_file=ENV_PATH, env_file_encoding="utf-8", extra="ignore")

# Instantiate settings to be imported across the application
settings = Settings()