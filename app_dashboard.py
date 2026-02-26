"""
Dashboard del Gap Generazionale Eurostat
---------------------------------
Un'applicazione Streamlit per visualizzare il divario economico tra le generazioni.
Si connette a un database PostgreSQL per recuperare dati Eurostat pre-elaborati.

Esegui con: streamlit run app_dashboard.py
"""

import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import logging

from db_config import get_db_engine
from country_codes import eurostat_dictionary

# Configura Logging
logging.basicConfig(level=logging.INFO)

# Configurazione Pagina
st.set_page_config(page_title="Analisi del Gap Generazionale", layout="wide")

@st.cache_resource
def get_db_engine_cached():
    """Restituisce una connessione al database engine nella cache."""
    return get_db_engine()

@st.cache_data
def get_data_from_db(query):
    """
    Esegue una query SQL e restituisce il risultato come DataFrame.
    """
    try:
        engine = get_db_engine_cached()
        return pd.read_sql(query, engine)
    except Exception as e:
        st.error(f"⚠️ SQL Query Error or Database Connection Failed:\n\n{str(e)}")
        # For debug purposes on cloud, also print the query that failed
        st.error(f"Query attempted:\n{query}")
        return pd.DataFrame()

def main():
    st.title("📊 Analisi del Gap Economico Generazionale")
    st.markdown("""
    Questa dashboard analizza i dati Eurostat per evidenziare le disparità economiche tra le fasce demografiche più giovani 
    e la popolazione generale/generazioni più anziane.
    """)

    # --- Filtri Sidebar ---
    st.sidebar.header("Filtri")
    
    # 1. Recupera prima paesi e anni disponibili (Query leggere)
    # Interroghiamo colonne specifiche per evitare di caricare interi dataset solo per i filtri
    # Nota: Usiamo 'unemployment' come base per il range paese/anno dato che è un dataset chiave
    try:
        filter_query = "SELECT DISTINCT TRIM(geo) AS geo, year FROM unemployment"
        df_filters = get_data_from_db(filter_query)
        
        if not df_filters.empty:
            all_countries = sorted(df_filters['geo'].unique(), key=lambda x: eurostat_dictionary.get(x, x))
            min_year = int(df_filters['year'].min())
            max_year = int(df_filters['year'].max())
        else:
            all_countries = []
            min_year, max_year = 2010, 2023
    except:
        # Fallback se la tabella non esiste ancora
        all_countries = []
        min_year, max_year = 2010, 2023
    
    # 2. Recupera fasce d'età disponibili per il filtro
    # Rimosso come richiesto dall'utente (Hardcoded Under 30 vs Totale)


    # Filtro Paese
    # Default su alcuni principali se disponibili
    default_selection = [c for c in ['IT', 'FR', 'DE', 'ES', 'EU27_2020'] if c in all_countries]
    if not default_selection and all_countries:
        default_selection = all_countries[:3]
        
    selected_countries = st.sidebar.multiselect(
        "Seleziona Paesi", 
        all_countries, 
        default=default_selection,
        format_func=lambda x: eurostat_dictionary.get(x, x)
    )
    
    if not selected_countries:
        st.warning("Seleziona almeno un paese.")
        return

    # Filtro Intervallo Anni
    selected_years = st.sidebar.slider("Seleziona Intervallo Anni", min_year, max_year, (min_year, max_year))

    # Filtro Età RIMOSSO
    # selected_ages = st.sidebar.multiselect("Seleziona Fasce d'Età (Disoccupazione/Povertà)", all_ages, default=default_age)
    
    # Formatta lista per clausola SQL IN
    countries_sql = "'" + "','".join(selected_countries) + "'"
    # ages_sql = "'" + "','".join(selected_ages) + "'"

    # --- Caricamento Dati con Query Esplicite ---
    
    # Query 1: Disoccupazione
    # Recupero solo dati rilevanti per i filtri selezionati
    query_unemp = f"""
        SELECT TRIM(geo) AS geo, year, TRIM(age) AS age, value 
        FROM unemployment 
        WHERE TRIM(geo) IN ({countries_sql}) 
        AND year BETWEEN {selected_years[0]} AND {selected_years[1]}
        AND TRIM(age) IN ('Y15-29', 'Y15-74') -- Under 30 vs Totale (Popolazione Attiva Y15-74 è standard per tasso Totale)
        AND TRIM(sex) = 'T'
        AND TRIM(unit) = 'PC_ACT'
    """
    df_unemp = get_data_from_db(query_unemp)

    # Query 2: Povertà
    # Recupero per l'ultimo anno selezionato
    query_poverty = f"""
        SELECT TRIM(geo) AS geo, year, 
               CASE 
                   WHEN TRIM(age) IN ('Y25-54', 'Y50-64') THEN 'Y25-64' -- Approssimazione per 30-70
                   ELSE TRIM(age) 
               END as age_group,
               TRIM(sex) AS sex, TRIM(unit) AS unit, AVG(value) as value
        FROM poverty_risk 
        WHERE TRIM(geo) IN ({countries_sql}) 
        AND year = {selected_years[1]}
        AND TRIM(age) IN ('Y16-29', 'Y25-54', 'Y50-64') -- Giovani vs Età Media
        AND TRIM(sex) = 'T'
        AND TRIM(unit) = 'PC' -- Percentuale
        GROUP BY geo, year, age_group, sex, unit
    """
    df_poverty = get_data_from_db(query_poverty)

    # Query 3: Uscita di Casa
    # Recupero per l'ultimo anno disponibile nell'intervallo (o semplicemente l'ultimo)
    query_home = f"""
        SELECT TRIM(geo) AS geo, year, value 
        FROM leaving_home 
        WHERE year = (SELECT MAX(year) FROM leaving_home WHERE year <= {selected_years[1]})
        AND TRIM(sex) = 'T'
        AND TRIM(unit) = 'AVG'
    """
    # Nota: Per l'uscita di casa potremmo voler vedere tutti i paesi per contesto, o solo i selezionati? 
    # Continuiamo a recuperare tutto per fare il confronto evidenziato.
    # La logica precedente mostrava tutti i paesi con evidenziazione.
    # Ottimizziamo recuperando tutto per l'ultimo anno per mantenere quella visuale.
    df_home = get_data_from_db(query_home)


    # --- Visualizzazioni ---
    
    # 💥 DEBUGGING UI 💥
    if st.sidebar.checkbox("Mostra Dati Grezzi (Local/Cloud Bug)"):
        st.write("### 🛠 DEBUG INFO")
        st.write("Dati grezzi estratti dal DB:")
        st.write("**Disoccupazione** `df_unemp`:", df_unemp.head(), "\nTipi:", df_unemp.dtypes)
        st.write("**Povertà** `df_poverty`:", df_poverty.head(), "\nTipi:", df_poverty.dtypes)
        st.write("**Casa** `df_home`:", df_home.head(), "\nTipi:", df_home.dtypes)
        st.write("Selezionati:", selected_countries)

    # 1. Analisi Disoccupazione
    # 1. Analisi Disoccupazione
    st.header("1. Disoccupazione: Giovani vs Totale")
    st.markdown("Confronto tra il tasso di disoccupazione dei giovani (15-29) e quello della popolazione totale.")
    
    if not df_unemp.empty:
        # I dati sono già filtrati dalla query SQL
        filtered_unemp = df_unemp
        
        # Assicura che 'value' e 'year' siano numerici, gestendo eventuali virgole
        filtered_unemp = filtered_unemp.copy()
        if filtered_unemp['value'].dtype == object:
            filtered_unemp['value'] = filtered_unemp['value'].astype(str).str.replace(',', '.')
        filtered_unemp['value'] = pd.to_numeric(filtered_unemp['value'], errors='coerce')
        filtered_unemp['year'] = pd.to_numeric(filtered_unemp['year'], errors='coerce')
        
        # Mappa codice geo al nome, rimuovendo gli spazi vuoti aggiuntivi del db
        filtered_unemp['geo'] = filtered_unemp['geo'].astype(str).str.strip()
        filtered_unemp['country_name'] = filtered_unemp['geo'].map(eurostat_dictionary).fillna(filtered_unemp['geo'])
        
        if not filtered_unemp.empty:
            # Rimuovi spazi nascosti
            filtered_unemp['age'] = filtered_unemp['age'].astype(str).str.strip()
            
            # Map robusto
            def map_age_unemp(a):
                if 'Y15-29' in a: return 'Giovani (15-29)'
                if 'Y15-74' in a: return 'Totale (15-74)'
                return a
            
            # Combina paese + età per avere serie dati indipendenti
            filtered_unemp['Serie'] = filtered_unemp['country_name'] + " - " + filtered_unemp['age_label']
            
            # Pivot the dataframe for st.line_chart
            pivot_unemp = filtered_unemp.pivot_table(index='year', columns='Serie', values='value').reset_index()
            pivot_unemp.set_index('year', inplace=True)
            
            st.line_chart(pivot_unemp, use_container_width=True)
            
        else:
            st.info("Nessun dato sulla disoccupazione disponibile per questa configurazione.")
    else:
        st.info("Nessun dato sulla disoccupazione disponibile.")

    # 2. Analisi Rischio Povertà
    # 2. Analisi Rischio Povertà
    st.header("2. Rischio di Povertà")
    st.markdown("Percentuale della popolazione a rischio di povertà per fascia d'età (Focus: Giovani vs Anziani).")
    
    if not df_poverty.empty:
        # I dati sono già filtrati dalla query SQL (per ultimo anno e paesi)
        filtered_pov = df_poverty.copy()
        
        # Assicura numericità per evitare che Plotly tratti i valori come categorie
        if filtered_pov['value'].dtype == object:
            filtered_pov['value'] = filtered_pov['value'].astype(str).str.replace(',', '.')
        filtered_pov['value'] = pd.to_numeric(filtered_pov['value'], errors='coerce')
        
        # Determina l'anno effettivamente recuperato (dai dati)
        latest_year = filtered_pov['year'].max() if not filtered_pov.empty else selected_years[1]
        
        if not filtered_pov.empty:
             filtered_pov['geo'] = filtered_pov['geo'].astype(str).str.strip()
             filtered_pov['country_name'] = filtered_pov['geo'].map(eurostat_dictionary).fillna(filtered_pov['geo'])
             
             # Map robusto
             filtered_pov['age_group'] = filtered_pov['age_group'].astype(str).str.strip()
             def map_age_pov(a):
                 if 'Y16-29' in a: return 'Giovani (16-29)'
                 if 'Y25-64' in a: return 'Adulti (25-64)'
                 return a
             filtered_pov['age_label'] = filtered_pov['age_group'].apply(map_age_pov)
             
             # Per avere i gruppi affiancati, facciamo pivot su Fascia Età
             pivot_pov = filtered_pov.pivot_table(index='country_name', columns='age_label', values='value').reset_index()
             pivot_pov.set_index('country_name', inplace=True)
             
             st.markdown(f"**Tasso di Rischio di Povertà ({latest_year})**")
             st.bar_chart(pivot_pov, use_container_width=True)
             
        else:
            st.warning(f"Nessun dato sulla povertà trovato per i paesi selezionati nell'anno {selected_years[1]}.")
    else:
        st.info("Nessun dato sulla povertà disponibile.")

    # 3. Età di Uscita dalla Casa dei Genitori
    # 3. Età di Uscita dalla Casa dei Genitori
    st.header("3. Età di Uscita dalla Casa dei Genitori")
    st.markdown("Età media stimata in cui i giovani lasciano il nucleo familiare.")
    
    if not df_home.empty:
        # Ultimo anno disponibile
        latest_home_year = df_home['year'].max()
        # Confronta paesi selezionati vs Media UE (se disponibile) o solo tra loro
        # Per contesto, mostriamo tutti i paesi ma evidenziamo i selezionati
        
        df_home = df_home.copy()
        if df_home['value'].dtype == object:
            df_home['value'] = df_home['value'].astype(str).str.replace(',', '.')
        df_home['value'] = pd.to_numeric(df_home['value'], errors='coerce')

        df_home['color'] = df_home['geo'].apply(lambda x: 'Selezionati' if str(x).strip() in selected_countries else 'Altri')
        df_home['geo'] = df_home['geo'].astype(str).str.strip()
        df_home['country_name'] = df_home['geo'].map(eurostat_dictionary).fillna(df_home['geo'])
        # Ordina per valore
        df_home_sorted = df_home.sort_values('value', ascending=False)
        df_home_sorted.set_index('country_name', inplace=True)
        
        st.markdown(f"**Età Media di Uscita di Casa ({latest_home_year})**")
        st.bar_chart(df_home_sorted[['value']], use_container_width=True)
    else:
        st.info("Nessun dato sull'età di uscita di casa.")

    # --- Conclusioni / Approfondimenti ---
    st.markdown("---")
    st.subheader("💡 Punti Chiave")
    st.info("""
    - **Gap Disoccupazione**: La disoccupazione giovanile è spesso significativamente più alta della media generale, indicando difficoltà nell'ingresso nel mercato del lavoro.
    - **Povertà**: In molti paesi, le generazioni più giovani affrontano un rischio di povertà uguale o superiore rispetto agli anziani, invertendo i trend storici.
    - **Indipendenza**: L'età di uscita di casa è un indicatore di stabilità economica. Età più elevate sono spesso correlate a un'alta disoccupazione giovanile e costi abitativi elevati.
    """)

if __name__ == "__main__":
    main()
