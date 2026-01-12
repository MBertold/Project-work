
import pandas as pd
from db_config import get_db_engine

def check_poverty_duplicates():
    engine = get_db_engine()
    
    print("\n--- Check Duplicate Poverty Data for Italy 2022 (Y16-29) ---")
    query = """
        SELECT year, age, value, sex, unit 
        FROM poverty_risk 
        WHERE geo = 'IT' 
        AND year = 2022 
        AND age = 'Y16-29' 
        AND sex = 'T'
    """
    try:
        df = pd.read_sql(query, engine)
        print(df)
    except Exception as e:
        print(e)
        
    print("\n--- Check Duplicate Poverty Data for Italy 2022 (TOTAL) ---")
    query_total = """
        SELECT year, age, value, sex, unit 
        FROM poverty_risk 
        WHERE geo = 'IT' 
        AND year = 2022 
        AND age = 'TOTAL' 
        AND sex = 'T'
    """
    try:
        df_t = pd.read_sql(query_total, engine)
        print(df_t)
    except Exception as e:
        print(e)

if __name__ == "__main__":
    check_poverty_duplicates()
