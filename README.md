# Swiss Rail Reliability

**DISS-DPA project, Fall 2026 – Claude Bernard Lyon 1 University**
Team: BENABIREZ Manar, MALKI Nada, MEFLAH Yousra

## Question

- **Overarching question:** which stations are both important for the Swiss rail network and often delayed, and can we see a disruption starting there in real time?
- **Part 1 (Volume):** which stations have the most delay, weighted by their importance, and how does punctuality change by hour, day of the week and canton?

In Part 1, the importance of a station is measured with three numbers:
- **trains per day** (different trains that stop there),
- **number of lines** that stop there,
- **total delay per day** = average arrival delay × trains per day.

A train is **on time** if it arrives less than 3 minutes late (SBB definition).

## Data

| Dataset | What it contains | Size used | Link |
|---|---|---|---|
| **Ist-Daten v2** | one row per vehicle at one stop: planned and real arrival/departure times | 15 daily CSV files, 28 Sep – 12 Oct 2026, **9.32 GB**, 36,412,778 rows | https://opendata.swiss/en/dataset/ist-daten-v2 |
| **Service Points v2** | official station list: ID, name, canton, coordinates | 157,737 rows (73,089 stations) | https://opendata.swiss/en/dataset/service-point-v2 |

- **Join:** on the station ID (`BPUIC` in Ist-Daten = `number` in Service Points).
- **Licence:** both datasets are published by opentransportdata.swiss under the opendata.swiss licence *"Open use. Must provide the source"* (https://opendata.swiss/terms-of-use#terms_by).
  Source: opentransportdata.swiss.
- **The data is not in this repository** (too big). The script `scripts/download_data.py` downloads it (see below).
- The online catalogue only keeps the most recent days. A copy of our raw files is on Kaggle: **https://www.kaggle.com/datasets/meflahyousra/swiss-rail-ist-daten-v2-28-sep-to-11-oct-2026**


## How to run

### 1. Requirements

- **Docker Desktop** (with Docker Compose). Nothing else: Python, Java and Spark are inside the Docker image.
- About **20 GB of free disk space** (raw CSV + Parquet tables).
- At least **8 GB of RAM**. Spark uses 4 GB by default. On an 8 GB laptop, create a file called `.env` next to `docker-compose.yml` with this line:
  ```
  SPARK_DRIVER_MEMORY=2g
  ```

### 2. Get the code

```
git clone https://github.com/ManarBenabirez/Swiss-Rail-Reliability-Project.git
cd Swiss-Rail-Reliability-Project
docker compose build
```

### 3. Get the data

Nothing to do: if `data/raw/` is empty, the pipeline (step 4) first downloads **our copy of the data from Kaggle** and puts it in the right folders. It can also be done alone:
```
docker compose run --rm pipeline python scripts/download_kaggle.py
```

Other option, from the official catalogue (only while it still has these days):
```
docker compose run --rm pipeline python scripts/download_data.py
```
Useful options: `--list` (days available online), `--days 1` (quick test with the most recent day).

Both scripts put the files here:
```
data/raw/istdaten/2026-09-28_IstDaten.csv   (one file per day)
data/raw/service_points/service_points.csv
```

### 4. Run the pipeline

```
docker compose run --rm pipeline
```

This downloads the data from Kaggle if needed, then runs the 6 steps one after the other (`run_pipeline.sh`) and stops at the first error.
You can also run one step alone, for example:
```
docker compose run --rm pipeline python src/02_clean.py
```

### 5. Look at the results

- The result tables are in `output/results/` and the charts in `output/figures/`.
- In **Jupyter** (after running the pipeline): `docker compose up jupyter`, open http://localhost:8888 and then `notebooks/explore_results.ipynb`.
- In the **terminal**, show any table:
  ```
  docker compose run --rm pipeline python scripts/show_table.py stops_clean 10 "station_name = 'Bern'"
  ```

## Pipeline steps

| Step | Script | What it does | Output |
|---|---|---|---|
| 0 | `scripts/download_data.py` | downloads the raw CSV files | `data/raw/` |
| 1 | `src/01_ingest.py` | reads the CSV with an explicit schema, converts the types, renames the columns | `data/parquet/stops_typed`, `stations_typed`, `output/ingestion_report.csv` |
| 2 | `src/01b_check_ingestion.py` | checks that Parquet = raw CSV (rows per day, types, sample rows) | PASS / FAIL on screen |
| 3 | `src/02a_profile.py` | measures the problems before cleaning (missing values, duplicates, outliers...) | `output/profile/*.csv` |
| 4 | `src/02_clean.py` | keeps Swiss train stops, removes duplicates and wrong rows, joins with the stations, computes the delay | `data/parquet/stops_clean`, `stations_clean`, `output/cleaning_report.csv` |
| 5 | `src/02b_check_stations_join.py` | checks the station list and the join (one row per station, canton coverage) | PASS / FAIL + `output/profile/join_*.csv` |
| 6 | `src/03_analyze.py` | answers the question with Spark SQL | `output/figures/*.png`|

## Results
- `output/figures/`: the 5 charts used in the report.

## Project structure

```
├── Dockerfile, docker-compose.yml, requirements.txt
├── run_pipeline.sh          runs steps 1 to 6
├── scripts/                 download, first look at the raw files, show a table
├── src/                     the pipeline (one script per step)
├── notebooks/               Jupyter notebook to explore the results
└── docs/                    report
```
