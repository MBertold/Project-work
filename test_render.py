from app_dashboard import get_db_engine, get_data_from_db
import altair as alt

selected_years = [2010, 2024]
selected_countries = ['IT', 'FR']

query_home = f"""
        SELECT TRIM(geo) AS geo, year, value 
        FROM leaving_home 
        WHERE year = (SELECT MAX(year) FROM leaving_home WHERE year <= {selected_years[1]})
        AND TRIM(sex) = 'T'
        AND TRIM(unit) = 'AVG'
"""
df_home = get_data_from_db(query_home)
if df_home['value'].dtype == object:
    df_home['value'] = df_home['value'].astype(str).str.replace(',', '.')
df_home['color'] = df_home['geo'].apply(lambda x: 'Selezionati' if str(x).strip() in selected_countries else 'Altri')
df_home['geo'] = df_home['geo'].astype(str).str.strip()
df_home['country_name'] = df_home['geo']
df_home_sorted = df_home.sort_values('value', ascending=False)
latest_home_year = 2024

try:
    chart_home = alt.Chart(df_home_sorted).mark_bar().encode(
        x=alt.X('country_name:N', sort=None, title='Paese'), 
        y=alt.Y('value:Q', title='Età (Anni)'),
        color=alt.Color('color:N', scale=alt.Scale(domain=['Selezionati', 'Altri'], range=['red', 'lightgray']), title='Legenda'),
        tooltip=['country_name', 'value']
    ).properties(title=f"Età Media di Uscita di Casa ({latest_home_year})")
    chart_home.to_dict()
    print("chart_home Valid")
except Exception as e:
    print(e)
