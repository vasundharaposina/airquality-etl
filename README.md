# Weather and Air-Quality ETL Pipeline

A small data-engineering project: pull hourly weather and air-quality data for
five Indian cities, clean and validate it, store it in SQLite, and explore it
in a Streamlit dashboard.

## How it works

```
Open-Meteo APIs  ->  Extract  ->  raw_payloads (original JSON)
 (weather + air)         |
                         v
                    Transform (Pandas: types, nulls, duplicates, past hours only)
                         |
                         v
                    Validate (range checks, rejected rows are counted)
                         |
                         v
                    readings table (one row per city per hour, upserted)
                         |
                         v
                    Streamlit dashboard + pipeline_runs log
```

- **Source:** [Open-Meteo](https://open-meteo.com/) weather and air-quality APIs (free, no API key).
- **Cities:** Bengaluru, Hyderabad, Delhi, Mumbai, Chennai (edit `CITIES` in `etl.py`).
- **Storage:** SQLite (`data/pipeline.db`) with three tables:
  - `raw_payloads` keeps the untouched API responses.
  - `readings` holds cleaned data, primary key `(city, observed_at)`.
  - `pipeline_runs` logs rows extracted, loaded and rejected for every city on every run.
- **Reliability:** HTTP retries with backoff, one failing city does not stop the run, and
  re-running is idempotent (upsert, no duplicates). Logs go to `data/pipeline.log`.
- **Data quality:** values outside sensible ranges (for example humidity above 100)
  are rejected and counted in `pipeline_runs.rows_rejected`.

## Run it

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt

python etl.py                    # run the pipeline
streamlit run dashboard.py       # open the dashboard
```

## Run it daily (optional)

- **Linux/macOS (cron):** `0 7 * * * cd /path/to/airquality-etl && .venv/bin/python etl.py`
- **Windows:** use Task Scheduler to run `python etl.py` once a day.

## Ideas to extend it

- Swap SQLite for PostgreSQL.
- Orchestrate with Airflow instead of cron.
- Package it with Docker.
- Add unit tests for `transform` and `validate`.
