import pandas as pd
from db_config import get_db_engine

def test():
    engine = get_db_engine()
    df = pd.read_sql("SELECT geo, year, age, value, sex, unit FROM unemployment WHERE age IN ('Y15-29', 'Y15-74') AND sex = 'T' AND unit = 'PC_ACT' LIMIT 5", engine)
    print("UNEMPLOYMENT SAMPLE:")
    print(df)
    print([repr(x) for x in df['age'].unique()])

    df2 = pd.read_sql("SELECT geo, year, age, value, sex, unit FROM poverty_risk WHERE age IN ('Y16-29', 'Y25-54', 'Y50-64') AND sex = 'T' AND unit = 'PC' LIMIT 5", engine)
    print("\nPOVERTY SAMPLE:")
    print(df2)
    print([repr(x) for x in df2['age'].unique()])

if __name__ == '__main__':
    test()
