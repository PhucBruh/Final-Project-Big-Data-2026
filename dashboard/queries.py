import duckdb

def get_db_connection():
    return duckdb.connect(database=':memory:')

def get_team_evolution(start_season, end_season, clutch_mode=False, teams=None):
    conn = get_db_connection()
    clutch_filter = "AND is_clutch = 1" if clutch_mode else ""
    query = f"""
    SELECT season, team, 
           CAST(SUM(total_possessions) AS FLOAT) / MAX(games_played) as pace,
           CASE WHEN SUM(total_possessions) > 0 THEN (CAST(SUM(points_scored) AS FLOAT) / SUM(total_possessions)) * 100 ELSE 0 END as ORtg,
           CASE WHEN SUM(def_possessions) > 0 THEN (CAST(SUM(points_allowed) AS FLOAT) / SUM(def_possessions)) * 100 ELSE 0 END as DRtg,
           CASE WHEN SUM(total_fga) > 0 THEN CAST(SUM(three_pt_attempts) AS FLOAT) / SUM(total_fga) ELSE 0 END as three_point_rate,
           CASE WHEN SUM(total_fga) > 0 THEN CAST(SUM(rim_attempts) AS FLOAT) / SUM(total_fga) ELSE 0 END as rim_rate,
           CASE WHEN SUM(total_fga) > 0 THEN CAST(SUM(midrange_attempts) AS FLOAT) / SUM(total_fga) ELSE 0 END as midrange_rate
    FROM 'data/processed/fact_team_metrics/*/*.parquet'
    WHERE season >= {start_season} AND season <= {end_season} {clutch_filter}
    """
    if teams:
        if isinstance(teams, list) and len(teams) > 0:
            team_list = "', '".join(teams)
            query += f" AND team IN ('{team_list}')"
        elif isinstance(teams, str):
            query += f" AND team = '{teams}'"
        
    query += " GROUP BY season, team ORDER BY season, team"
    return conn.execute(query).df()

def get_league_evolution(start_season, end_season, clutch_mode=False, teams=None):
    conn = get_db_connection()
    clutch_filter = "AND is_clutch = 1" if clutch_mode else ""
    
    team_filter = ""
    if teams:
        if isinstance(teams, list) and len(teams) > 0:
            team_list = "', '".join(teams)
            team_filter = f"AND team IN ('{team_list}')"
        elif isinstance(teams, str):
            team_filter = f"AND team = '{teams}'"
            
    query = f"""
    SELECT season, 
           CAST(SUM(total_possessions) AS FLOAT) / MAX(games_played) as pace,
           CASE WHEN SUM(total_possessions) > 0 THEN (CAST(SUM(points_scored) AS FLOAT) / SUM(total_possessions)) * 100 ELSE 0 END as ORtg,
           CASE WHEN SUM(def_possessions) > 0 THEN (CAST(SUM(points_allowed) AS FLOAT) / SUM(def_possessions)) * 100 ELSE 0 END as DRtg,
           CASE WHEN SUM(total_fga) > 0 THEN CAST(SUM(three_pt_attempts) AS FLOAT) / SUM(total_fga) ELSE 0 END as three_point_rate,
           CASE WHEN SUM(total_fga) > 0 THEN CAST(SUM(rim_attempts) AS FLOAT) / SUM(total_fga) ELSE 0 END as rim_rate,
           CASE WHEN SUM(total_fga) > 0 THEN CAST(SUM(midrange_attempts) AS FLOAT) / SUM(total_fga) ELSE 0 END as midrange_rate
    FROM 'data/processed/fact_team_metrics/*/*.parquet'
    WHERE season >= {start_season} AND season <= {end_season} {clutch_filter} {team_filter}
    GROUP BY season
    ORDER BY season
    """
    return conn.execute(query).df()

def get_player_stats(player, start_season, end_season, clutch_mode=False):
    conn = get_db_connection()
    clutch_filter = "AND is_clutch = 1" if clutch_mode else ""
    query = f"""
    SELECT season,
           CASE WHEN SUM(fga) + 0.44 * SUM(fta) > 0 THEN CAST(SUM(pts) AS FLOAT) / (2 * (SUM(fga) + 0.44 * SUM(fta))) ELSE 0 END as TS_pct,
           CASE WHEN SUM(fga) > 0 THEN (CAST(SUM(fgm) AS FLOAT) + 0.5 * SUM(fg3m)) / SUM(fga) ELSE 0 END as eFG_pct,
           CASE WHEN SUM(fg3a) > 0 THEN CAST(SUM(fg3m) AS FLOAT) / SUM(fg3a) ELSE 0 END as three_point_efficiency
    FROM 'data/processed/fact_player_metrics/*/*.parquet'
    WHERE player = '{player}' AND season >= {start_season} AND season <= {end_season} {clutch_filter}
    GROUP BY season
    ORDER BY season
    """
    return conn.execute(query).df()

def get_league_player_stats(start_season, end_season, clutch_mode=False):
    conn = get_db_connection()
    clutch_filter = "AND is_clutch = 1" if clutch_mode else ""
    query = f"""
    SELECT season,
           CASE WHEN SUM(fga) + 0.44 * SUM(fta) > 0 THEN CAST(SUM(pts) AS FLOAT) / (2 * (SUM(fga) + 0.44 * SUM(fta))) ELSE 0 END as TS_pct,
           CASE WHEN SUM(fga) > 0 THEN (CAST(SUM(fgm) AS FLOAT) + 0.5 * SUM(fg3m)) / SUM(fga) ELSE 0 END as eFG_pct,
           CASE WHEN SUM(fg3a) > 0 THEN CAST(SUM(fg3m) AS FLOAT) / SUM(fg3a) ELSE 0 END as three_point_efficiency
    FROM 'data/processed/fact_player_metrics/*/*.parquet'
    WHERE season >= {start_season} AND season <= {end_season} {clutch_filter}
    GROUP BY season
    ORDER BY season
    """
    return conn.execute(query).df()

def get_team_shot_heatmap(teams, start_season, end_season, is_clutch=False, clutch_time=5, clutch_margin=5):
    conn = get_db_connection()
    clutch_filter = f"AND period >= 4 AND seconds_remaining <= {clutch_time * 60} AND score_margin >= -{clutch_margin} AND score_margin <= {clutch_margin}" if is_clutch else ""
    
    team_filter = ""
    if teams:
        if isinstance(teams, list) and len(teams) > 0:
            team_list = "', '".join(teams)
            team_filter = f"AND team IN ('{team_list}')"
        elif isinstance(teams, str):
            team_filter = f"AND team = '{teams}'"
            
    query = f"""
    SELECT x, y, made_flag, shot_zone, team
    FROM 'data/processed/fact_shots/*/*.parquet'
    WHERE season >= {start_season} AND season <= {end_season} {clutch_filter} {team_filter}
    """
    return conn.execute(query).df()

def get_player_shot_heatmap(player, start_season, end_season, is_clutch=False, clutch_time=5, clutch_margin=5):
    conn = get_db_connection()
    clutch_filter = f"AND period >= 4 AND seconds_remaining <= {clutch_time * 60} AND score_margin >= -{clutch_margin} AND score_margin <= {clutch_margin}" if is_clutch else ""
    query = f"""
    SELECT x, y, made_flag, shot_zone
    FROM 'data/processed/fact_shots/*/*.parquet'
    WHERE shooter = '{player}' AND season >= {start_season} AND season <= {end_season} {clutch_filter}
    """
    return conn.execute(query).df()

def get_player_shot_profile(player, start_season, end_season, clutch_mode=False, clutch_time=5, clutch_margin=5):
    conn = get_db_connection()
    clutch_filter = f"AND period >= 4 AND seconds_remaining <= {clutch_time * 60} AND score_margin >= -{clutch_margin} AND score_margin <= {clutch_margin}" if clutch_mode else ""
    query = f"""
    SELECT season,
           CAST(SUM(CASE WHEN shot_zone = 'Restricted Area' THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*) as rim_rate,
           CAST(SUM(CASE WHEN shot_zone = 'Midrange' THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*) as midrange_rate,
           CAST(SUM(CASE WHEN shot_zone IN ('Corner 3', 'Above Break 3') OR shot_zone LIKE '%3%' THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*) as three_point_rate
    FROM 'data/processed/fact_shots/*/*.parquet'
    WHERE shooter = '{player}' AND season >= {start_season} AND season <= {end_season} {clutch_filter}
    GROUP BY season
    ORDER BY season
    """
    return conn.execute(query).df()

def get_all_teams():
    conn = get_db_connection()
    try:
        return conn.execute("SELECT DISTINCT team FROM 'data/processed/fact_team_metrics/*/*.parquet' WHERE team IS NOT NULL ORDER BY team").df()['team'].tolist()
    except:
        return []

def get_all_players():
    conn = get_db_connection()
    try:
        return conn.execute("SELECT DISTINCT player FROM 'data/processed/fact_player_metrics/*/*.parquet' WHERE player IS NOT NULL ORDER BY player").df()['player'].tolist()
    except:
        return []
