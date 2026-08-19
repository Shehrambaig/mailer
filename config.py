import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    SECRET_KEY = os.getenv("FLASK_SECRET_KEY", "dev-secret-key")
    SQLALCHEMY_DATABASE_URI = os.getenv("DATABASE_URL", "postgresql://localhost:5432/mailer")
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    # OpenLetterConnect — defaults to the demo environment until a live
    # account exists; set OLC_BASE_URL to the production URL to go live.
    OLC_API_KEY = os.getenv("OLC_API_KEY")
    OLC_BASE_URL = os.getenv("OLC_BASE_URL", "https://demoapi.openletterconnect.com/api/v1")

    # Cloudflare
    CLOUDFLARE_ACCOUNT_ID = os.getenv("CLOUDFLARE_ACCOUNT_ID")
    CLOUDFLARE_API_TOKEN = os.getenv("CLOUDFLARE_API_TOKEN")


class DevelopmentConfig(Config):
    DEBUG = True


class ProductionConfig(Config):
    DEBUG = False


config = {
    "development": DevelopmentConfig,
    "production": ProductionConfig,
    "default": DevelopmentConfig,
}
