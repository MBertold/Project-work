"""
Database Configuration
----------------------
Centralized configuration for PostgreSQL connection.
"""

from sqlalchemy import create_engine
import logging


from dotenv import load_dotenv
import os

from dotenv import load_dotenv
import os
import streamlit as st

# Load environment variables from .env file
load_dotenv()

def get_db_config():
    """Retrieves database configuration from st.secrets (priority) or .env."""
    # 1. Try Streamlit Secrets
    try:
        if st.secrets and "postgres" in st.secrets:
            return st.secrets["postgres"]
    except FileNotFoundError:
        pass # Not running in Streamlit or no secrets.toml
    except Exception:
         pass

    # 2. Fallback to Environment Variables
    return {
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT', '5432'),
        'dbname': os.getenv('DB_NAME'),
        'sslmode': os.getenv('DB_SSLMODE', 'require')
    }

def get_db_engine():
    """Creates and returns a SQLAlchemy engine."""
    config = get_db_config()
    
    # Construct URL based on source
    # Secrets usually come as dict, os.getenv as string. 
    # Ensure all required keys exist
    required_keys = ['user', 'password', 'host', 'dbname']
    if not all(key in config and config[key] for key in required_keys):
        # logging.error("Missing database configuration details.")
        # Only log if strictly necessary to avoid noise during build
        pass

    # Handle port type (int vs string)
    port = str(config.get('port', 5432))
    sslmode = config.get('sslmode', 'require')
    
    url = f"postgresql://{config['user']}:{config['password']}@{config['host']}:{port}/{config['dbname']}?sslmode={sslmode}"
    
    try:
        engine = create_engine(url)
        return engine
    except Exception as e:
        logging.error(f"Failed to create database engine: {e}")
        raise

