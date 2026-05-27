# NBA Big Data Analytics Platform

## Project Overview

This project processes large-scale NBA play-by-play event data (1997–2026) to reconstruct possessions, extract advanced basketball metrics, and serve a low-latency analytical dashboard.

## Data Source

The raw play-by-play dataset and shot logs can be downloaded from Kaggle.

- **Source:** [NBA Play-by-Play Data (1997-2026) (by Szymon Jóźwiak)](https://www.kaggle.com/datasets/szymonjwiak/nba-play-by-play-data-1997-2023)
- Place the downloaded CSV files inside the `data/raw/` directory before running the ETL pipeline.

## Tech Stack

- **Data Engineering:** PySpark (ETL), Parquet (Storage)
- **Serving Layer:** DuckDB
- **Visualization:** Streamlit, Plotly, Streamlit-Antd-Components, Streamlit-Extras

## Architecture

1. **Ingestion:** Reads raw CSV files using PySpark.
2. **Event Cleaning:** Normalizes coordinates, parses game clock, rebuilds scores.
3. **Possession Reconstruction:** Uses window functions to identify ball possession boundaries.
4. **Feature Engineering:** Calculates advanced team and player metrics.
5. **Storage:** Writes optimized parquet files partitioned by season.
6. **Analytics Dashboard:** DuckDB directly queries parquet for real-time interactivity.

## Setup & Run

### 1. Install Dependencies

```bash
# Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows use: .venv\Scripts\activate

# Install required packages
pip install -r requirements.txt
```

### 2. Prepare Data

- Download the dataset from the Kaggle link provided above.
- Extract the downloaded archive.
- Move the CSV files (`play_by_play.csv`, `team_metadata.csv`, `player_metadata.csv`) into the `data/raw/` directory. Create this directory if it doesn't exist.

### 3. Run the ETL Pipeline

```bash
python etl/pipeline.py
```

### 4. Run the Dashboard

```bash
streamlit run dashboard/app.py
```
