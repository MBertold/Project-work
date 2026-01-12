"""
Database Configuration
----------------------
Centralized configuration for PostgreSQL connection.
"""

from sqlalchemy import create_engine
import logging


from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Database Configuration using Environment Variables
DB_CONFIG = {
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT', '5432'),
    'dbname': os.getenv('DB_NAME'),
    'sslmode': os.getenv('DB_SSLMODE', 'require')
}

DATABASE_URL = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}?sslmode={DB_CONFIG['sslmode']}"


def get_db_engine():
    """Creates and returns a SQLAlchemy engine."""
    try:
        engine = create_engine(DATABASE_URL)
        return engine
    except Exception as e:
        logging.error(f"Failed to create database engine: {e}")
        raise
