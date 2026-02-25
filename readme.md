# Analisi del Gap Economico Generazionale (Eurostat)

Questo progetto implementa una pipeline dati (ETL) e una dashboard interattiva per l'analisi delle disparità economiche tra le fasce demografiche più giovani e la popolazione generale in Europa, basata sui dati aperti di **Eurostat**.

## Componenti del Progetto

Il progetto è suddiviso principalmente in due script essenziali:

1. **`etl_eurostat.py` - Script ETL**
   Scarica, pulisce ed elabora i dati grezzi direttamente da Eurostat e li carica in un database PostgreSQL.
   I dataset trattati includono:
   - **Tasso di disoccupazione per età** (une_rt_a)
   - **Tasso di rischio povertà** (ilc_li02)
   - **Età media di uscita dalla casa dei genitori** (yth_demo_030)
   - **Tasso di sovraccarico del costo dell'alloggio** (tessi161)

2. **`app_dashboard.py` - Dashboard Analitica**
   Un'applicazione web creata in **Streamlit** che si connette al database PostgreSQL per visualizzare le intuizioni tramite grafici interattivi, offrendo filtri per anno e per paese, in modo da esplorare il divario economico-generazionale.

3. **File di Supporto**
   - **`db_config.py`**: Configurazione della stringa di connessione e interazione col database PostgreSQL.
   - **`country_codes.py`**: Mappatura personalizzata tra i codici di Eurostat (es. 'IT', 'FR') e i corrispondenti nomi completi.
   - **`requirements.txt`**: Elenco delle dipendenze di progetto.

## Installazione

1. Clona o scarica il progetto
2. Crea un virtual environment e installa le librerie necessarie:
   ```bash
   pip install -r requirements.txt
   ```
3. Configura correttamente un'istanza PostgreSQL e aggiorna, se necessario, le credenziali in `db_config.py`.

## Esecuzione

### 1. Inizializzazione Database (Processo ETL)
Scarica i dati più recenti ed esegui la pipeline ETL lanciando lo script apposito:
```bash
python etl_eurostat.py
```
*I dati verranno salvati all'interno delle tabelle del database PostgreSQL.*

### 2. Esecuzione Dashboard
Una volta che i dati sono salvati sul database, avvia la dashboard di analisi interattiva:
```bash
streamlit run app_dashboard.py
```
