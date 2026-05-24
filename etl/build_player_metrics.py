from pyspark.sql.functions import col, when, sum as spark_sum, count

def build_fact_player_metrics(df, fact_shots):
    # Calculate points and attempts from the main event stream
    player_events = df.groupBy("player", "season", "is_clutch").agg(
        spark_sum(when(col("type") == "Made Shot", 
                       when(col("desc").like("%3PT%"), 3).otherwise(2))
                  .when((col("type") == "Free Throw") & (col("desc").like("% PTS%")), 1)
                  .otherwise(0)).alias("pts"),
        spark_sum(when(col("type").isin("Made Shot", "Missed Shot"), 1).otherwise(0)).alias("fga"),
        spark_sum(when(col("type") == "Made Shot", 1).otherwise(0)).alias("fgm"),
        spark_sum(when((col("type") == "Made Shot") & (col("desc").like("%3PT%")), 1).otherwise(0)).alias("fg3m"),
        spark_sum(when((col("type").isin("Made Shot", "Missed Shot")) & (col("desc").like("%3PT%")), 1).otherwise(0)).alias("fg3a"),
        spark_sum(when(col("type") == "Free Throw", 1).otherwise(0)).alias("fta"),
        spark_sum(when(col("type") == "Turnover", 1).otherwise(0)).alias("tov")
    ).filter(col("player").isNotNull() & (col("player") != ""))
    
    # Actually we can just select these directly
    fact_player_metrics = player_events.select(
        "player", "season", "is_clutch", "pts", "fga", "fgm", "fg3m", "fg3a", "fta", "tov"
    )
    
    return fact_player_metrics
