
import pandas as pd
from db_config import get_db_engine

def check_poverty_age_codes():
    engine = get_db_engine()
    
    print("\n--- Poverty Age Codes List ---")
    try:
        df_p = pd.read_sql("SELECT DISTINCT age FROM poverty_risk", engine)
        codes = sorted(df_p['age'].tolist())
        print(codes)
        
        # Check if there are any that overlap 30-70
        potential_matches = [c for c in codes if '25' in c or '30' in c or '45' in c or '50' in c or '65' in c or '75' in c]
        print("\nPossible matches for 30-70 range:")
        print(potential_matches)
    except Exception as e:
        print(e)

if __name__ == "__main__":
    check_poverty_age_codes()
