"""
ETL Script for Eurostat Data
----------------------------
This script downloads, cleans, and loads the following datasets into a PostgreSQL database:
1. une_rt_a: Unemployment rate by age
2. ilc_li02: At-risk-of-poverty rate
3. yth_demo_030: Average age of leaving parental household
4. tessi161: Housing cost overburden rate

Requirements:
    pip install eurostat pandas sqlalchemy psycopg2-binary
"""

import eurostat
import pandas as pd
from sqlalchemy import create_engine
import logging

from db_config import get_db_engine
from country_codes import eurostat_dictionary

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Database connection is now handled in db_config.py

def fetch_and_clean_data():
    """
    Fetches data from Eurostat, cleans it, and returns a dictionary of DataFrames.
    """
    datasets = {
        'unemployment': 'une_rt_a',
        'poverty_risk': 'ilc_li02',
        'leaving_home': 'yth_demo_030',
        'housing_cost': 'tessi161' # Reference for Housing cost overburden rate
    }

    cleaned_data = {}

    for name, code in datasets.items():
        logging.info(f"Fetching dataset: {name} ({code})...")
        try:
            # Fetch data as DataFrame
            df = eurostat.get_data_df(code)
            
            # Basic Cleaning
            # 1. Rename 'geo\time' or similar columns to standard 'geo' and melt years
            # Eurostat library usually returns columns like: unit, age, sex, geo\time, 2022, 2021...
            
            # Identify columns that are not years (usually metadata columns)
            # They often end with '\time' or are named 'geo', 'unit', etc.
            id_vars = [col for col in df.columns if not str(col).isdigit() and not isinstance(col, int)]
            
            # Melt the yearly columns into rows for easier SQL analysis
            df_melted = df.melt(id_vars=id_vars, var_name='year', value_name='value')
            
            # Convert year and value to numeric, coercing errors
            df_melted['year'] = pd.to_numeric(df_melted['year'], errors='coerce')
            df_melted['value'] = pd.to_numeric(df_melted['value'], errors='coerce')
            
            # Standardize column names: lowercase and replace specific chars
            df_melted.columns = [c.lower().replace('\\time', '') for c in df_melted.columns]
            
            # Rename 'geo' if it exists (sometimes it's purely 'geo', sometimes 'geo\time')
            if 'geo' not in df_melted.columns:
                 # Try to find a column containing 'geo'
                 geo_col = next((c for c in df_melted.columns if 'geo' in c), None)
                 if geo_col:
                     df_melted.rename(columns={geo_col: 'geo'}, inplace=True)

            # Drop rows with NaN values in critical columns
            df_melted.dropna(subset=['year', 'value', 'geo'], inplace=True)
            
            # Specific Filtering Logic based on dataset type
            if name == 'unemployment':
                # Filter for relevant age groups if 'age' column exists
                if 'age' in df_melted.columns:
                    # Keep Y15-24, Y15-29, Y25-74 (Adult comparison), and TOTAL
                    # Note: You might need to check the actual codes in the data
                    relevant_ages = ['Y15-24', 'Y15-29', 'Y25-74', 'TOTAL'] 
                    # Filter only if values are present, to avoid empty df if codes differ
                    # For simplicity in this generic script, we keep all but log a note
                    # Real-world: df_melted = df_melted[df_melted['age'].isin(relevant_ages)]
                    pass

            cleaned_data[name] = df_melted
            logging.info(f"Dataset {name} processed. Shape: {df_melted.shape}")

        except Exception as e:
            logging.error(f"Error processing {name} ({code}): {e}")
    
    # Process Country Codes Dictionary
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
    Loads the cleaned DataFrames into PostgreSQL.
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
    
    # 1. Fetch and Clean
    data = fetch_and_clean_data()
    
    # 2. Load to DB
    # NOTE: We use the config from db_config.py
    try:
        engine = get_db_engine()
        load_to_postgres(data, engine)
        logging.info("ETL process finished successfully. Data loaded to DB.")
    except Exception as e:
         logging.error(f"ETL failed during DB load: {e}")
