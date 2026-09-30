# Weather ETL Pipeline

![Hourly Weather ETL](https://github.com/Amir-Mirkazemi/weather-ETL-pipeline/actions/workflows/hourly_etl.yml/badge.svg)

Automated ETL pipeline extracting live weather data hourly via GitHub Actions, with fail-loud error handling and typed SQLite storage.

## What it does

- **Extract** — calls the [Open-Meteo API](https://open-meteo.com/) (no API key required) for current temperature and humidity from a rotating list of world cities — a different country each day, picked deterministically from the date, so the dataset isn't just one fixed location.
- **Transform** — casts the raw JSON response into typed fields (float temperature, integer humidity) and attaches a timestamp.
- **Load** — inserts the record into a local SQLite database (`weather_data.db`), creating the `weather` table automatically on first run.
- **Orchestrate** — a GitHub Actions workflow runs the script hourly; there's no server or external scheduler involved.

## Architecture

```mermaid
flowchart LR
    A[GitHub Actions\nhourly trigger] --> B[main.py]
    B -->|Extract| C[Open-Meteo API]
    C -->|JSON response| B
    B -->|Transform + Load| D[(SQLite\nweather_data.db)]
```

## Design choices

- **Zero-cost infrastructure** — Open-Meteo needs no API key, and GitHub Actions provides the scheduler, so the whole pipeline runs for free with nothing to host.
- **Fails loudly, not silently** — any exception (network failure, unexpected API response, etc.) is caught, logged, and the script exits with a non-zero status, so a broken run shows up as a failed Actions job rather than quietly skipping a load.
- **Idempotent setup** — `CREATE TABLE IF NOT EXISTS` means the pipeline runs safely on a fresh checkout with no manual setup step.
- **Deterministic rotation, not random** — the city for a given day is picked with `day_of_year % len(LOCATIONS)`, so the same day always maps to the same city (reproducible and easy to verify), instead of a random pick that could vary between runs on the same day.

## Tech stack

Python · `requests` · `sqlite3` · GitHub Actions (hourly schedule)

## Possible next steps

- ~~Parameterize the city instead of hardcoding it~~ — done: rotates through 7 countries by date
- ~~Add a basic data-quality check (reject clearly invalid temperature/humidity values) before insert~~ — done
- Swap SQLite for a cloud warehouse (e.g. Snowflake) as a closer analog to production-scale ingestion
