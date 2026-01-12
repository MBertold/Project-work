"""
Dashboard del Gap Generazionale Eurostat
---------------------------------
Un'applicazione Streamlit per visualizzare il divario economico tra le generazioni.
Si connette a un database PostgreSQL per recuperare dati Eurostat pre-elaborati.

Esegui con: streamlit run app_dashboard.py
"""

import streamlit as st
import pandas as pd
import plotly.express as px
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
        st.error(f"Error executing query: {query}\nError: {e}")
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
        filter_query = "SELECT DISTINCT geo, year FROM unemployment"
        df_filters = get_data_from_db(filter_query)
        
        if not df_filters.empty:
            all_countries = sorted(df_filters['geo'].unique(), key=lambda x: eurostat_dictionary.get(x, x))
            min_year = int(df_filters['year'].min())
            max_year = int(df_filters['year'].max())
        else:
            all_countries = []
            min_year, max_year = 2010, 2023
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
        SELECT geo, year, age, value 
        FROM unemployment 
        WHERE geo IN ({countries_sql}) 
        AND year BETWEEN {selected_years[0]} AND {selected_years[1]}
        AND age IN ('Y15-29', 'Y15-74') -- Under 30 vs Totale (Popolazione Attiva Y15-74 è standard per tasso Totale)
        AND sex = 'T'
        AND unit = 'PC_ACT'
    """
    df_unemp = get_data_from_db(query_unemp)

    # Query 2: Povertà
    # Recupero per l'ultimo anno selezionato
    query_poverty = f"""
        SELECT geo, year, 
               CASE 
                   WHEN age IN ('Y25-54', 'Y50-64') THEN 'Y25-64' -- Approssimazione per 30-70
                   ELSE age 
               END as age_group,
               sex, unit, AVG(value) as value
        FROM poverty_risk 
        WHERE geo IN ({countries_sql}) 
        AND year = {selected_years[1]}
        AND age IN ('Y16-29', 'Y25-54', 'Y50-64') -- Giovani vs Età Media
        AND sex = 'T'
        AND unit = 'PC' -- Percentuale
        GROUP BY geo, year, age_group, sex, unit
    """
    df_poverty = get_data_from_db(query_poverty)

    # Query 3: Uscita di Casa
    # Recupero per l'ultimo anno disponibile nell'intervallo (o semplicemente l'ultimo)
    query_home = f"""
        SELECT geo, year, value 
        FROM leaving_home 
        WHERE year = (SELECT MAX(year) FROM leaving_home WHERE year <= {selected_years[1]})
        AND sex = 'T'
        AND unit = 'AVG'
    """
    # Nota: Per l'uscita di casa potremmo voler vedere tutti i paesi per contesto, o solo i selezionati? 
    # Continuiamo a recuperare tutto per fare il confronto evidenziato.
    # La logica precedente mostrava tutti i paesi con evidenziazione.
    # Ottimizziamo recuperando tutto per l'ultimo anno per mantenere quella visuale.
    df_home = get_data_from_db(query_home)


    # --- Visualizzazioni ---

    # 1. Analisi Disoccupazione
    # 1. Analisi Disoccupazione
    st.header("1. Disoccupazione: Giovani vs Totale")
    st.markdown("Confronto tra il tasso di disoccupazione dei giovani (15-29) e quello della popolazione totale.")
    
    if not df_unemp.empty:
        # I dati sono già filtrati dalla query SQL
        filtered_unemp = df_unemp
        
        # Assicura che 'value' sia numerico
        filtered_unemp = filtered_unemp.copy()
        filtered_unemp['value'] = pd.to_numeric(filtered_unemp['value'], errors='coerce')
        
        # Mappa codice geo al nome
        filtered_unemp['country_name'] = filtered_unemp['geo'].map(eurostat_dictionary).fillna(filtered_unemp['geo'])
        
        if not filtered_unemp.empty:
            # Mappa codici a etichette leggibili
            label_map = {'Y15-29': 'Giovani (15-29)', 'Y15-74': 'Totale (15-74)'}
            filtered_unemp['age_label'] = filtered_unemp['age'].map(label_map).fillna(filtered_unemp['age'])
            
            fig_unemp = px.line(
                filtered_unemp, 
                x='year', 
                y='value', 
                color='country_name', 
                line_dash='age_label', 
                title='Tasso di Disoccupazione (Giovani vs Totale)',
                labels={'value': 'Tasso di Disoccupazione (%)', 'year': 'Anno', 'country_name': 'Paese', 'age_label': 'Fascia Età'}
            )
        st.plotly_chart(fig_unemp, width='stretch') # Fixed deprecation
    else:
        st.info("Nessun dato sulla disoccupazione disponibile.")

    # 2. Analisi Rischio Povertà
    # 2. Analisi Rischio Povertà
    st.header("2. Rischio di Povertà")
    st.markdown("Percentuale della popolazione a rischio di povertà per fascia d'età (Focus: Giovani vs Anziani).")
    
    if not df_poverty.empty:
        # I dati sono già filtrati dalla query SQL (per ultimo anno e paesi)
        filtered_pov = df_poverty
        
        # Determina l'anno effettivamente recuperato (dai dati)
        latest_year = filtered_pov['year'].max() if not filtered_pov.empty else selected_years[1]
        
        if not filtered_pov.empty:
             filtered_pov['country_name'] = filtered_pov['geo'].map(eurostat_dictionary).fillna(filtered_pov['geo'])
        if not filtered_pov.empty:
             filtered_pov['country_name'] = filtered_pov['geo'].map(eurostat_dictionary).fillna(filtered_pov['geo'])
             
             # Map codes to readable labels
             label_map_pov = {'Y16-29': 'Giovani (16-29)', 'Y25-64': 'Adulti (25-64)'}
             filtered_pov['age_label'] = filtered_pov['age_group'].map(label_map_pov).fillna(filtered_pov['age_group'])
             
             fig_pov = px.bar(
                filtered_pov, 
                x='country_name', 
                y='value', 
                color='age_label', 
                barmode='group',
                title=f'Tasso di Rischio di Povertà ({latest_year})',
                labels={'value': 'Tasso (%)', 'country_name': 'Paese', 'age_label': 'Fascia Età'}
            )
             st.plotly_chart(fig_pov, width='stretch') # Fixed deprecation
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
        
        df_home['color'] = df_home['geo'].apply(lambda x: 'Selezionati' if x in selected_countries else 'Altri')
        df_home['country_name'] = df_home['geo'].map(eurostat_dictionary).fillna(df_home['geo'])
        # Ordina per valore
        df_home_sorted = df_home.sort_values('value', ascending=False)

        fig_home = px.bar(
            df_home_sorted,
            x='country_name',
            y='value',
            color='color',
            title=f"Età Media di Uscita di Casa ({latest_home_year})",
            labels={'value': 'Età (Anni)', 'country_name': 'Paese'},
            color_discrete_map={'Selezionati': 'red', 'Altri': 'lightgrey'}
        )
        st.plotly_chart(fig_home, width='stretch')
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
