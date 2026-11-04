"""
Configurazione Database
----------------------
Configurazione centralizzata per la connessione PostgreSQL locale, letta dal file .env.
"""

from sqlalchemy import create_engine, URL
from dotenv import load_dotenv
import logging
import os

# Carica variabili d'ambiente dal file .env
load_dotenv()

def get_db_config():
    """Recupera la configurazione del database dalle variabili d'ambiente (.env)."""
    return {
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT', '5432'),
        'dbname': os.getenv('DB_NAME'),
        # 'prefer' usa SSL se il server lo supporta, altrimenti si connette senza
        'sslmode': os.getenv('DB_SSLMODE', 'prefer')
    }

def get_db_engine():
    """Crea e restituisce un engine SQLAlchemy."""
    config = get_db_config()

    required_keys = ['user', 'password', 'host', 'dbname']
    missing = [key for key in required_keys if not config.get(key)]
    if missing:
        raise ValueError(
            f"Configurazione database incompleta, mancano: {', '.join(missing)}. "
            "Imposta le variabili DB_USER, DB_PASSWORD, DB_HOST e DB_NAME nel file .env (vedi README)."
        )

    # URL.create gestisce caratteri speciali nella password (es. '@', ':')
    url = URL.create(
        drivername="postgresql",
        username=config['user'],
        password=config['password'],
        host=config['host'],
        port=int(config['port']),
        database=config['dbname'],
        query={"sslmode": config['sslmode']},
    )

    try:
        return create_engine(url)
    except Exception as e:
        logging.error(f"Failed to create database engine: {e}")
        raise
