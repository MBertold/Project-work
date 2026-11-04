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

## Esecuzione in locale

### Prerequisiti
- **Python 3.11** o superiore
- **PostgreSQL** (installato in locale oppure tramite Docker, vedi sotto)
- Connessione a internet (l'ETL scarica i dati dalle API di Eurostat)

### 1. Clona il progetto e installa le dipendenze
```bash
git clone https://github.com/MBertold/Project-work.git
cd Project-work
python -m venv venv
```
Attiva il virtual environment:
- **Windows (PowerShell)**: `venv\Scripts\Activate.ps1`
- **macOS / Linux**: `source venv/bin/activate`

Poi installa le librerie:
```bash
pip install -r requirements.txt
```

### 2. Avvia PostgreSQL
**Opzione A – Docker** (la più rapida):
```bash
docker run --name eurostat-db -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=eurostat -p 5432:5432 -d postgres:16
```
Per riavviarlo in seguito: `docker start eurostat-db`.

**Opzione B – PostgreSQL installato in locale**: crea un database vuoto, ad esempio:
```sql
CREATE DATABASE eurostat;
```

### 3. Configura la connessione al database
Le credenziali **non** vanno scritte in `db_config.py`: vengono lette da un file `.env` nella cartella del progetto (già escluso da git). Crealo con questo contenuto, adattando i valori alla tua installazione:
```ini
DB_USER=postgres
DB_PASSWORD=postgres
DB_HOST=localhost
DB_PORT=5432
DB_NAME=eurostat
DB_SSLMODE=disable
```
> **Importante:** in locale imposta `DB_SSLMODE=disable`. Il valore predefinito è `require`, pensato per i database in cloud, e fa fallire la connessione a un PostgreSQL locale senza SSL.

In alternativa (ad es. su Streamlit Cloud) la dashboard legge le stesse credenziali da `.streamlit/secrets.toml`, che ha la precedenza sul `.env`:
```toml
[postgres]
user = "postgres"
password = "postgres"
host = "localhost"
port = 5432
dbname = "eurostat"
sslmode = "disable"
```

### 4. Carica i dati (processo ETL)
```bash
python etl_eurostat.py
```
Lo script scarica i quattro dataset da Eurostat e crea le tabelle `unemployment`, `poverty_risk`, `leaving_home`, `housing_cost` e `country_codes` (se esistono già vengono sovrascritte). Richiede circa un minuto; il dataset sulla povertà supera il milione di righe. Rilancialo quando vuoi aggiornare i dati.

### 5. Avvia la dashboard
```bash
streamlit run app_dashboard.py
```
Si apre automaticamente nel browser all'indirizzo http://localhost:8501.

### Risoluzione problemi
- **`SSL is not enabled on the server` / errore di connessione**: controlla di aver impostato `DB_SSLMODE=disable` nel `.env`.
- **`password authentication failed` o `could not connect to server`**: verifica che PostgreSQL sia avviato e che i valori del `.env` siano corretti.
- **La dashboard mostra "Nessun dato…" o `relation "unemployment" does not exist`**: le tabelle non esistono ancora; esegui prima `python etl_eurostat.py` e controlla nel suo log che ogni tabella risulti `Successfully loaded`.
