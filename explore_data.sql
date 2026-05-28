-- FACT SHOTS
SELECT
  *
FROM
  'data/processed/fact_shots/**/*.parquet'
LIMIT
  100;

-- FACT POSSESSIONS
SELECT
  *
FROM
  'data/processed/fact_possessions/**/*.parquet'
LIMIT
  100;

-- FACT TEAM METRICS
SELECT
  *
FROM
  'data/processed/fact_team_metrics/**/*.parquet'
ORDER BY
  season DESC,
  total_possessions DESC
LIMIT
  100;

-- FACT PLAYER METRICS
SELECT
  *
FROM
  'data/processed/fact_player_metrics/**/*.parquet'
ORDER BY
  season DESC,
  pts DESC
LIMIT
  100;

-- ANALYTICS EXAMPLE: TS% Calculation
SELECT
  player,
  season,
  pts,
  fga,
  fta,
  ROUND(pts / (2.0 * (fga + 0.44 * fta)), 3) AS true_shooting_pct
FROM
  'data/processed/fact_player_metrics/**/*.parquet'
WHERE
  player LIKE '%L. James%'
  AND is_clutch = 0
ORDER BY
  season DESC
LIMIT
  10;

-- ANALYTICS EXAMPLE 2: Team ORtg and Pace (Golden State Warriors Dynasty)
SELECT
  season,
  team,
  total_possessions,
  points_scored,
  ROUND((points_scored * 100.0) / total_possessions, 1) AS offensive_rating,
  ROUND(
    (total_possessions * 48.0) / (games_played * 48.0),
    1
  ) AS pace
FROM
  'data/processed/fact_team_metrics/**/*.parquet'
WHERE
  team = 'GSW'
  AND is_clutch = 0
ORDER BY
  season DESC
LIMIT
  10;

-- ANALYTICS EXAMPLE 3: The "Pace and Space" Era (League 3PT Evolution)
SELECT
  season,
  SUM(three_pt_attempts) AS total_3pt_attempts,
  SUM(midrange_attempts) AS total_midrange_attempts,
  ROUND(
    SUM(three_pt_attempts) * 100.0 / SUM(total_fga),
    1
  ) AS three_pt_pct_of_offense
FROM
  'data/processed/fact_team_metrics/**/*.parquet'
WHERE
  is_clutch = 0
GROUP BY
  season
ORDER BY
  season DESC;

-- DASHBOARD QUERY 1: Team Evolution (Lấy dữ liệu vẽ biểu đồ Line/Radar cho Đội)
SELECT
  season,
  team,
  CAST(SUM(total_possessions) AS FLOAT) / MAX(games_played) AS pace,
  CASE
    WHEN SUM(total_possessions) > 0 THEN (
      CAST(SUM(points_scored) AS FLOAT) / SUM(total_possessions)
    ) * 100
    ELSE 0
  END AS ORtg,
  CASE
    WHEN SUM(def_possessions) > 0 THEN (
      CAST(SUM(points_allowed) AS FLOAT) / SUM(def_possessions)
    ) * 100
    ELSE 0
  END AS DRtg,
  CASE
    WHEN SUM(total_fga) > 0 THEN CAST(SUM(three_pt_attempts) AS FLOAT) / SUM(total_fga)
    ELSE 0
  END AS three_point_rate,
  CASE
    WHEN SUM(total_fga) > 0 THEN CAST(SUM(midrange_attempts) AS FLOAT) / SUM(total_fga)
    ELSE 0
  END AS midrange_rate
FROM
  'data/processed/fact_team_metrics/**/*.parquet'
WHERE
  season >= 2015
  AND season <= 2024
  AND is_clutch = 0
  AND team = 'BOS'
GROUP BY
  season,
  team
ORDER BY
  season;

-- DASHBOARD QUERY 2: Player Advanced Stats (Lấy dữ liệu vẽ thẻ KPI cho Cầu thủ)
SELECT
  season,
  CASE
    WHEN SUM(fga) + 0.44 * SUM(fta) > 0 THEN CAST(SUM(pts) AS FLOAT) / (2 * (SUM(fga) + 0.44 * SUM(fta)))
    ELSE 0
  END AS TS_pct,
  CASE
    WHEN SUM(fga) > 0 THEN (CAST(SUM(fgm) AS FLOAT) + 0.5 * SUM(fg3m)) / SUM(fga)
    ELSE 0
  END AS eFG_pct,
  CASE
    WHEN SUM(fg3a) > 0 THEN CAST(SUM(fg3m) AS FLOAT) / SUM(fg3a)
    ELSE 0
  END AS three_point_efficiency
FROM
  'data/processed/fact_player_metrics/**/*.parquet'
WHERE
  player = 'S. Curry'
  AND season >= 2015
  AND season <= 2024
  AND is_clutch = 0
GROUP BY
  season
ORDER BY
  season;

-- DASHBOARD QUERY 3: Shot Chart Coordinates (Lấy hàng ngàn tọa độ XY để vẽ Heatmap)
SELECT
  x,
  y,
  made_flag,
  shot_zone,
  team
FROM
  'data/processed/fact_shots/**/*.parquet'
WHERE
  season = 2024
  AND team = 'BOS'
  AND is_clutch = 0
LIMIT
  5000;