You are a senior data engineer, analytics engineer, and sports analytics visualization expert helping me build a UNIVERSITY BIG DATA PROJECT using the NBA Play-by-Play dataset (1997–2023).

==================================================
PROJECT TITLE
==================================================

Big Data Analytics of NBA Team Systems and Player Evolution (1997–2023)

==================================================
PROJECT GOAL
==================================================

Build a COMPLETE BIG DATA ANALYTICS PIPELINE that:

- processes large-scale NBA play-by-play event data
- performs distributed ETL using PySpark
- reconstructs basketball possessions from event streams
- stores optimized parquet analytical tables
- serves low-latency analytical queries using DuckDB
- visualizes insights using a PROFESSIONAL Streamlit dashboard

IMPORTANT:

This is NOT:

- a machine learning project
- a recommendation system
- a generic dashboard tutorial

This IS:

- a data engineering project
- a big data analytics pipeline
- an event-stream analytics system
- an interactive analytical storytelling platform

==================================================
TECH STACK
==================================================

Data Engineering:

- PySpark
- DuckDB
- Parquet

Visualization:

- Streamlit
- Plotly

Optional UI Enhancements:

- streamlit-extras
- hydralit-components
- streamlit-option-menu

==================================================
SYSTEM ARCHITECTURE
==================================================

Raw CSV
↓
PySpark Distributed ETL
↓
Analytical Parquet Tables
↓
DuckDB Query Layer
↓
Streamlit Analytics Dashboard

IMPORTANT:

- Spark is ONLY used for ETL and preprocessing.
- Streamlit NEVER runs Spark jobs directly.
- DuckDB queries parquet directly.
- Dashboard interaction must remain responsive.

==================================================
MAIN ENGINEERING OBJECTIVES
==================================================

The project must demonstrate:

- distributed processing
- Spark window functions
- event sequence processing
- state reconstruction
- parquet optimization
- analytical query architecture
- interactive analytics

Avoid:

- deep learning
- neural networks
- recommendation systems
- unnecessary AI complexity

==================================================
REQUIRED PROJECT FOLDER STRUCTURE
==================================================

The project MUST follow a clean production-style structure.

Structure:

nba-bigdata-project/
│
├── data/
│ ├── raw/
│ └── processed/
│
├── etl/
│ ├── ingest.py
│ ├── clean_events.py
│ ├── reconstruct_possessions.py
│ ├── build_team_metrics.py
│ ├── build_player_metrics.py
│ └── pipeline.py
│
├── dashboard/
│ ├── app.py
│ ├── team_dashboard.py
│ ├── player_dashboard.py
│ ├── queries.py
│ ├── charts.py
│ └── utils.py
│
├── notebooks/
│ └── exploration.ipynb
│
├── reports/
│ ├── final_report.pdf
│ └── presentation.pptx
│
├── requirements.txt
├── README.md
└── docker-compose.yml (optional)

==================================================
CORE EVENT-STREAM ANALYTICS TASKS
==================================================

The dataset is NOT just tabular statistics.

It is a TEMPORAL EVENT STREAM.

The project must process:

- ordered basketball events
- possession boundaries
- scoring sequences
- clutch situations
- temporal event chains

==================================================
CRITICAL ETL REQUIREMENTS
==================================================

==================================================
STEP 1 — DATA INGESTION
==================================================

Tasks:

- read CSV using PySpark
- define schema explicitly
- validate corrupted rows
- handle missing values
- optimize Spark partitions

==================================================
STEP 2 — EVENT CLEANING
==================================================

Tasks:

1. Coordinate Cleaning

- keep x/y only for shot events
- remove invalid coordinates
- normalize coordinates

2. Time Processing

- convert game clock into seconds_remaining

3. Score Processing

- reconstruct score progression
- forward-fill score values
- calculate score_margin

4. Event Parsing

- extract assister names
- classify event types
- normalize textual events

==================================================
STEP 3 — POSSESSION RECONSTRUCTION
==================================================

IMPORTANT:
This is the MAIN BIG DATA ENGINEERING TASK.

Possessions do NOT exist explicitly in the dataset.

You must reconstruct possessions using:

- made shots
- turnovers
- rebounds
- fouls
- free throw sequences
- jump balls
- quarter transitions

Use:

- Spark window functions
- lag/lead
- ordered event processing
- temporal partitioning

Generate:

- possession_id
- possession_length
- offensive_team
- defensive_team
- possession_result

==================================================
STEP 4 — FEATURE ENGINEERING
==================================================

Generate advanced analytical features:

1. Shot Classification

- Restricted Area
- Midrange
- Corner 3
- Above Break 3

2. Team Metrics

- Pace
- Offensive Rating
- Defensive Rating
- 3PT Rate
- Rim Rate
- Midrange Rate

3. Player Metrics

- TS%
- eFG%
- Assist Rate
- 3PT Efficiency

4. Clutch Metrics
   Apply:

- period >= 4
- seconds_remaining <= 300
- score_margin <= 5

5. Temporal Metrics

- rolling efficiency
- scoring runs
- possession trends

==================================================
STEP 5 — PARQUET STORAGE OPTIMIZATION
==================================================

Store analytical tables using parquet.

Use:

- partitionBy("season")

Optimize:

- storage size
- analytical query speed
- filtering performance

==================================================
ANALYTICAL TABLES
==================================================

TABLE — fact_shots
Columns:

- shooter
- team
- x
- y
- shot_zone
- made_flag
- shot_value
- season

TABLE — fact_possessions
Columns:

- possession_id
- game_id
- team
- possession_length
- possession_result
- points_scored

TABLE — fact_team_metrics
Columns:

- team
- season
- pace
- ORtg
- DRtg
- three_point_rate
- rim_rate
- midrange_rate

TABLE — fact_player_metrics
Columns:

- player
- season
- TS_pct
- eFG_pct
- assist_rate
- three_point_efficiency

==================================================
DUCKDB QUERY LAYER
==================================================

DuckDB must:

- query parquet directly
- support low-latency analytical queries
- power dashboard interaction

IMPORTANT:
Do NOT load raw CSV into Streamlit.

==================================================
DASHBOARD PHILOSOPHY
==================================================

The dashboard should feel:

- modern
- analytical
- smooth
- focused
- presentation-ready

The dashboard should NOT feel:

- cluttered
- overloaded
- like a generic admin panel
- like a beginner Streamlit app

==================================================
DASHBOARD STRUCTURE
==================================================

ONLY TWO MAIN DASHBOARDS:

1. Team Analytics Dashboard
2. Player Analytics Dashboard

==================================================
GLOBAL SIDEBAR CONTROLS
==================================================

Include:

1. Season Range Slider
   1997 → 2023

IMPORTANT:
Support MULTI-SEASON analysis.

2. Era Quick Filters

- Late 90s
- 2000s
- Pace & Space Era
- Modern NBA

3. Team Selector

- searchable
- multi-team comparison

4. Player Selector

- searchable

5. Shot Zone Filters

- Restricted Area
- Midrange
- Corner 3
- Above Break 3

6. Clutch Mode Toggle

==================================================
DASHBOARD 1 — TEAM ANALYTICS
==================================================

CORE QUESTION:
“How did NBA offensive systems evolve over time?”

==================================================
SECTION A — LEAGUE EVOLUTION
==================================================

CHART 1 — Shot Distribution Evolution

Interactive stacked area chart.

Show:

- rim attempts
- midrange attempts
- corner 3 attempts
- above break 3 attempts

Purpose:
Reveal:

- decline of midrange offense
- rise of 3PT offense
- tactical evolution

IMPORTANT:
This is the PRIMARY STORYTELLING CHART.

---

CHART 2 — Pace Evolution

Interactive line chart.

Show:

- league pace trends
- team pace trends
- era comparison

==================================================
SECTION B — TEAM IDENTITY
==================================================

LEFT PANEL:
Team Shot Heatmap

Interactive NBA half-court density heatmap.

Features:

- compare teams
- compare eras
- clutch filtering

---

RIGHT PANEL:
Offensive Profile Radar

Metrics:

- Pace
- 3PT Rate
- Assist Ratio
- Rim Frequency
- Midrange Frequency

Purpose:
Show offensive identity visually.

==================================================
SECTION C — TEAM EFFICIENCY
==================================================

CHART 3 — ORtg vs DRtg Scatter

Interactive bubble chart.

X-axis:

- Offensive Rating

Y-axis:

- Defensive Rating

Bubble size:

- wins

Purpose:
Identify balanced elite teams.

---

CHART 4 — Team Metrics Table

Interactive sortable table.

Columns:

- pace
- ORtg
- DRtg
- TS%
- 3PT rate

==================================================
SECTION D — TEMPORAL EVOLUTION
==================================================

CHART 5 — Animated Team Evolution

Animated Plotly visualization.

Slider:
1997 → 2023

Animate:

- pace
- shot profile
- ORtg
- DRtg

Purpose:
Create strong presentation/demo impact.

==================================================
DASHBOARD 2 — PLAYER ANALYTICS
==================================================

CORE QUESTION:
“How do players evolve across their careers?”

==================================================
SECTION A — PLAYER OVERVIEW
==================================================

Top KPI Cards:

- TS%
- eFG%
- PPG
- Assist Rate

==================================================
SECTION B — CAREER EVOLUTION
==================================================

LEFT PANEL:
Career Efficiency Curves

Metrics:

- TS%
- eFG%
- usage

---

RIGHT PANEL:
Shot Profile Evolution

Stacked bar chart:

- rim attempts
- midrange attempts
- 3PT attempts

Purpose:
Reveal role evolution.

==================================================
SECTION C — PLAYER SHOT ANALYTICS
==================================================

Interactive Player Shot Heatmap

Features:

- shot efficiency by location
- clutch filtering
- made/missed density

==================================================
SECTION D — PLAYER DEVELOPMENT
==================================================

LEFT PANEL:
Improvement Trajectory

Metrics:

- TS% growth
- 3PT growth
- assist growth

---

RIGHT PANEL:
Breakout Season Detection

Highlight:

- breakout years
- efficiency jumps

==================================================
STREAMLIT OPTIMIZATION REQUIREMENTS
==================================================

Use:

- st.cache_data
- session state
- reusable chart components
- responsive columns
- Plotly animations

Avoid:

- endless vertical scrolling
- default Streamlit styling
- expensive recomputation

==================================================
PERFORMANCE BENCHMARKS
==================================================

Include comparisons for:

1. Pandas vs Spark ETL runtime
2. CSV vs Parquet storage size
3. DuckDB query latency

Explain:

- Spark distributed processing advantages
- parquet compression benefits
- DuckDB analytical performance

==================================================
EXPECTED FINAL RESULT
==================================================

The final project should feel like:

- a serious big data engineering project
- a professional sports analytics platform
- an analytical storytelling system

It must clearly demonstrate:

- distributed ETL processing
- event-stream reconstruction
- possession analytics
- parquet optimization
- analytical query serving
- interactive multi-season analytics
