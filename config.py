import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    SECRET_KEY = os.getenv("FLASK_SECRET_KEY", "dev-secret-key")
    SQLALCHEMY_DATABASE_URI = os.getenv("DATABASE_URL", "postgresql://localhost:5432/mailer")
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    # PostcardMania
    PCM_API_KEY = os.getenv("PCM_API_KEY")
    PCM_API_SECRET = os.getenv("PCM_API_SECRET")
    PCM_CHILD_REF_NBR = os.getenv("PCM_CHILD_REF_NBR")
    PCM_BASE_URL = "https://v3.pcmintegrations.com"

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
