"""
Configurazione Database
----------------------
Configurazione centralizzata per la connessione PostgreSQL.
"""

from sqlalchemy import create_engine
import logging


from dotenv import load_dotenv
import os
import streamlit as st

# Carica variabili d'ambiente dal file .env
load_dotenv()

def get_db_config():
    """Recupera configurazione database da st.secrets (priorità) o .env."""
    # 1. Prova Streamlit Secrets
    try:
        if st.secrets and "postgres" in st.secrets:
            return st.secrets["postgres"]
    except FileNotFoundError:
        pass # Non in esecuzione in Streamlit o secrets.toml mancante
    except Exception:
         pass

    # 2. Fallback su Variabili d'Ambiente
    return {
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT', '5432'),
        'dbname': os.getenv('DB_NAME'),
        'sslmode': os.getenv('DB_SSLMODE', 'require')
    }

def get_db_engine():
    """Crea e restituisce un engine SQLAlchemy."""
    config = get_db_config()
    
    # Costruisci URL basato sulla sorgente
    # Secrets solitamente arriva come dict, os.getenv come stringa. 
    # Assicura che tutte le chiavi richieste esistano
    required_keys = ['user', 'password', 'host', 'dbname']
    missing = [key for key in required_keys if not config.get(key)]
    if missing:
        raise ValueError(
            f"Configurazione database incompleta, mancano: {', '.join(missing)}. "
            "Imposta le variabili DB_USER, DB_PASSWORD, DB_HOST e DB_NAME nel file .env (vedi README)."
        )

    # Gestisci tipo porta (int vs string)
    port = str(config.get('port', 5432))
    sslmode = config.get('sslmode', 'require')
    
    url = f"postgresql://{config['user']}:{config['password']}@{config['host']}:{port}/{config['dbname']}?sslmode={sslmode}"
    
    try:
        engine = create_engine(url)
        return engine
    except Exception as e:
        logging.error(f"Failed to create database engine: {e}")
        raise

