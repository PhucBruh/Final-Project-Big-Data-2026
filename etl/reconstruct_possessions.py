from pyspark.sql import Window
from pyspark.sql.functions import col, when, last, lag, sum as spark_sum, min as spark_min, max as spark_max, concat_ws

def reconstruct_possessions(df):
    window_order = Window.partitionBy("gameid").orderBy("period", col("seconds_remaining").desc())
    
    offensive_actions = ["Made Shot", "Missed Shot", "Turnover", "Free Throw", "Rebound", "Jump Ball"]
    
    df = df.withColumn(
        "action_team",
        when(col("type").isin(offensive_actions), col("team")).otherwise(None)
    )
    
    df = df.withColumn(
        "possession_team",
        last("action_team", ignorenulls=True).over(window_order)
    )
    
    df = df.withColumn(
        "prev_possession_team",
        lag("possession_team").over(window_order)
    )
    df = df.withColumn(
        "prev_period",
        lag("period").over(window_order)
    )
    
    df = df.withColumn(
        "is_new_possession",
        when(col("possession_team").isNull(), 0)
        .when(col("possession_team") != col("prev_possession_team"), 1)
        .when(col("period") != col("prev_period"), 1)
        .when(col("prev_possession_team").isNull(), 1)
        .otherwise(0)
    )
    
    df = df.withColumn(
        "possession_id_game",
        spark_sum("is_new_possession").over(window_order)
    )
    
    df = df.withColumn("possession_id", concat_ws("_", col("gameid"), col("possession_id_game")))
    
    # Calculate points for each event
    df = df.withColumn(
        "points_scored_event",
        when(col("type") == "Made Shot", 
             when(col("desc").like("%3 PTS%"), 3).otherwise(2))
        .when((col("type") == "Free Throw") & (col("desc").like("% PTS%")), 1)
        .otherwise(0)
    )
    
    return df

def build_fact_possessions(df):
    turnovers = df.withColumn("is_turnover", when(col("type") == "Turnover", 1).otherwise(0)) \
                  .groupBy("possession_id").agg(spark_max("is_turnover").alias("has_turnover"))
                  
    possessions = df.groupBy("possession_id", "gameid", "possession_team").agg(
        (spark_max("seconds_remaining") - spark_min("seconds_remaining")).alias("possession_length"),
        spark_sum("points_scored_event").alias("points_scored"),
        spark_max("is_clutch").alias("is_clutch")
    )
    
    possessions = possessions.join(turnovers, "possession_id", "left")
    
    possessions = possessions.withColumn(
        "possession_result",
        when(col("points_scored") > 0, "Score")
        .when(col("has_turnover") == 1, "Turnover")
        .otherwise("Miss/Other")
    ).drop("has_turnover")
    
    possessions = possessions.withColumnRenamed("possession_team", "team")
    
    return possessions
