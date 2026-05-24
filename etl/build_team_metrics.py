from pyspark.sql.functions import col, when, sum as spark_sum, count, countDistinct, lit

def classify_shots(df):
    df = df.withColumn(
        "shot_zone",
        when(col("type").isin("Made Shot", "Missed Shot"),
             when(col("desc").like("%3PT%"),
                  when(col("y") <= 140, "Corner 3").otherwise("Above Break 3"))
             .when(col("dist") <= 4, "Restricted Area")
             .otherwise("Midrange")
        ).otherwise(None)
    )
    
    df = df.withColumn(
        "shot_value",
        when(col("type") == "Made Shot",
             when(col("desc").like("%3PT%"), 3).otherwise(2)
        ).otherwise(0)
    )
    
    df = df.withColumn(
        "made_flag",
        when(col("type") == "Made Shot", 1)
        .when(col("type") == "Missed Shot", 0)
        .otherwise(None)
    )
    
    # is_clutch has been moved to clean_events.py
    
    return df

def build_fact_shots(df):
    shots = df.filter(col("type").isin("Made Shot", "Missed Shot"))
    fact_shots = shots.select(
        col("player").alias("shooter"),
        col("team"),
        col("x"),
        col("y"),
        col("shot_zone"),
        col("made_flag"),
        col("shot_value"),
        col("season"),
        col("is_clutch"),
        col("period"),
        col("seconds_remaining"),
        col("score_margin")
    )
    return fact_shots

def build_fact_team_metrics(df, fact_possessions, fact_shots):
    game_season = df.select("gameid", "season").dropDuplicates()
    poss_with_season = fact_possessions.join(game_season, "gameid", "left")
    
    # 1. TÍNH CHỈ SỐ TẤN CÔNG (Offense)
    team_offense = poss_with_season.groupBy("team", "season", "is_clutch").agg(
        count("possession_id").alias("total_possessions"),
        spark_sum("points_scored").alias("points_scored"),
        countDistinct("gameid").alias("games_played")
    )
    
    # 2. TÍNH CHỈ SỐ PHÒNG NGỰ (Defense)
    game_teams = poss_with_season.select("gameid", col("team").alias("off_team")).dropDuplicates()
    
    matchups = game_teams.alias("a").join(
        game_teams.alias("b"), 
        (col("a.gameid") == col("b.gameid")) & (col("a.off_team") != col("b.off_team"))
    ).select(col("a.gameid"), col("a.off_team"), col("b.off_team").alias("def_team"))
    
    # Gắn đối thủ vào từng pha bóng
    poss_with_def = poss_with_season.join(
        matchups, 
        (poss_with_season.gameid == matchups.gameid) & (poss_with_season.team == matchups.off_team),
        "left"
    )
    
    # Aggregate điểm bị mất (points_allowed) cho đội phòng ngự
    team_defense = poss_with_def.groupBy(col("def_team").alias("team"), "season", "is_clutch").agg(
        count("possession_id").alias("def_possessions"),
        spark_sum("points_scored").alias("points_allowed")
    )
    
    # Merge Offense và Defense
    team_metrics = team_offense.join(team_defense, ["team", "season", "is_clutch"], "outer").fillna(0)
    
    shots_agg = fact_shots.groupBy("team", "season", "is_clutch").agg(
        count("*").alias("total_fga"),
        spark_sum(when(col("shot_zone").isin("Corner 3", "Above Break 3") | col("shot_zone").like("%3%"), 1).otherwise(0)).alias("three_pt_attempts"),
        spark_sum(when(col("shot_zone") == "Restricted Area", 1).otherwise(0)).alias("rim_attempts"),
        spark_sum(when(col("shot_zone") == "Midrange", 1).otherwise(0)).alias("midrange_attempts")
    )
    
    # Combine
    final_df = team_metrics.join(shots_agg, ["team", "season", "is_clutch"], "left").fillna(0)
    
    # CHÚ Ý: BÂY GIỜ CHÚNG TA LƯU RAW COUNTS ĐỂ DUCKDB TÍNH TOÁN ON THE FLY CHO CLUTCH TOGGLE
    return final_df.select(
        "team", "season", "is_clutch", "games_played", "total_possessions", "points_scored", 
        "def_possessions", "points_allowed", "three_pt_attempts", "rim_attempts", "midrange_attempts", "total_fga"
    )
