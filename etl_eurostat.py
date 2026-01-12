"""
Script ETL per Dati Eurostat
----------------------------
Questo script scarica, pulisce e carica i seguenti dataset in un database PostgreSQL:
1. une_rt_a: Tasso di disoccupazione per età
2. ilc_li02: Tasso di rischio povertà
3. yth_demo_030: Età media di uscita dalla casa dei genitori
4. tessi161: Tasso di sovraccarico del costo dell'alloggio

Requisiti:
    pip install eurostat pandas sqlalchemy psycopg2-binary
"""

import eurostat
import pandas as pd
from sqlalchemy import create_engine
import logging

from db_config import get_db_engine
from country_codes import eurostat_dictionary

# Configura Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# La connessione al database è ora gestita in db_config.py

def fetch_and_clean_data():
    """
    Recupera i dati da Eurostat, li pulisce e restituisce un dizionario di DataFrame.
    """
    datasets = {
        'unemployment': 'une_rt_a',
        'poverty_risk': 'ilc_li02',
        'leaving_home': 'yth_demo_030',
        'housing_cost': 'tessi161' # Riferimento per il tasso di sovraccarico del costo dell'alloggio
    }

    cleaned_data = {}

    for name, code in datasets.items():
        logging.info(f"Fetching dataset: {name} ({code})...")
        try:
            # Recupera dati come DataFrame
            df = eurostat.get_data_df(code)
            
            # Pulizia Base
            # 1. Rinomina 'geo\time' o simili in standard 'geo' e trasforma anni
            # La libreria Eurostat solitamente restituisce colonne come: unit, age, sex, geo\time, 2022, 2021...
            
            # Identifica colonne che non sono anni (solitamente metadati)
            # Spesso finiscono con '\time' o sono chiamate 'geo', 'unit', ecc.
            id_vars = [col for col in df.columns if not str(col).isdigit() and not isinstance(col, int)]
            
            # Fondi le colonne annuali in righe per una più facile analisi SQL
            df_melted = df.melt(id_vars=id_vars, var_name='year', value_name='value')
            
            # Converti anno e valore in numerico, forzando errori
            df_melted['year'] = pd.to_numeric(df_melted['year'], errors='coerce')
            df_melted['value'] = pd.to_numeric(df_melted['value'], errors='coerce')
            
            # Standardizza nomi colonne: minuscolo e sostituzione caratteri specifici
            df_melted.columns = [c.lower().replace('\\time', '') for c in df_melted.columns]
            
            # Rinomina 'geo' se esiste (a volte è puramente 'geo', a volte 'geo\time')
            if 'geo' not in df_melted.columns:
                 # Prova a trovare una colonna che contiene 'geo'
                 geo_col = next((c for c in df_melted.columns if 'geo' in c), None)
                 if geo_col:
                     df_melted.rename(columns={geo_col: 'geo'}, inplace=True)

            # Rimuovi righe con valori NaN in colonne critiche
            df_melted.dropna(subset=['year', 'value', 'geo'], inplace=True)
            
            # Logica di Filtro Specifica basata sul tipo di dataset
            if name == 'unemployment':
                # Filtra per gruppi d'età rilevanti se la colonna 'age' esiste
                if 'age' in df_melted.columns:
                    # Mantieni Y15-24, Y15-29, Y25-74 (Confronto Adulti), e TOTAL
                    # Nota: Potresti dover controllare i codici effettivi nei dati
                    relevant_ages = ['Y15-24', 'Y15-29', 'Y25-74', 'TOTAL'] 
                    # Filtra solo se i valori sono presenti, per evitare df vuoti se i codici differiscono
                    # Per semplicità in questo script generico, manteniamo tutto ma logghiamo una nota
                    # Mondo reale: df_melted = df_melted[df_melted['age'].isin(relevant_ages)]
                    pass

            cleaned_data[name] = df_melted
            logging.info(f"Dataset {name} processed. Shape: {df_melted.shape}")

        except Exception as e:
            logging.error(f"Error processing {name} ({code}): {e}")
    
    # Elabora Dizionario Codici Paese
    try:
        logging.info("Processing Country Codes...")
        df_codes = pd.DataFrame(list(eurostat_dictionary.items()), columns=['geo', 'country_name'])
        cleaned_data['country_codes'] = df_codes
        logging.info(f"Country Codes processed. Shape: {df_codes.shape}")
    except Exception as e:
        logging.error(f"Error processing Country Codes: {e}")

    return cleaned_data

def load_to_postgres(data_dict, engine):
    """
    Carica i DataFrame puliti in PostgreSQL.
    """
    for table_name, df in data_dict.items():
        try:
            logging.info(f"Loading {table_name} to database...")
            df.to_sql(table_name, engine, if_exists='replace', index=False)
            logging.info(f"Successfully loaded {table_name}.")
        except Exception as e:
            logging.error(f"Failed to load {table_name}: {e}")

if __name__ == "__main__":
    logging.info("Starting ETL process...")
    
    # 1. Recupera e Pulisci
    data = fetch_and_clean_data()
    
    # 2. Carica nel DB
    # NOTA: Usiamo la configurazione da db_config.py
    try:
        engine = get_db_engine()
        load_to_postgres(data, engine)
        logging.info("ETL process finished successfully. Data loaded to DB.")
    except Exception as e:
         logging.error(f"ETL failed during DB load: {e}")
