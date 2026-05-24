from pyspark.sql.functions import col, when, regexp_extract, max as spark_max
from pyspark.sql import Window

def clean_events(df):
    # 1. Coordinate Cleaning
    df = df.withColumn(
        "x",
        when(col("type").isin("Made Shot", "Missed Shot"), col("x")).otherwise(None)
    ).withColumn(
        "y",
        when(col("type").isin("Made Shot", "Missed Shot"), col("y")).otherwise(None)
    )
    
    # 2. Time Processing (PT12M00.00S -> 12 * 60 + 0)
    df = df.withColumn(
        "clock_minutes", regexp_extract(col("clock"), r"PT(\d+)M", 1).cast("int")
    ).withColumn(
        "clock_seconds", regexp_extract(col("clock"), r"M(\d+\.\d+)S", 1).cast("double")
    ).withColumn(
        "seconds_remaining", col("clock_minutes") * 60 + col("clock_seconds")
    ).drop("clock_minutes", "clock_seconds")
    
    # 3. Score Processing
    # We need a stable ordering. period asc, seconds_remaining desc
    window_score = Window.partitionBy("gameid") \
        .orderBy("period", col("seconds_remaining").desc()) \
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        
    df = df.withColumn("h_pts_ff", spark_max("h_pts").over(window_score)) \
           .withColumn("a_pts_ff", spark_max("a_pts").over(window_score))
           
    df = df.withColumn("h_pts_ff", when(col("h_pts_ff").isNull(), 0.0).otherwise(col("h_pts_ff"))) \
           .withColumn("a_pts_ff", when(col("a_pts_ff").isNull(), 0.0).otherwise(col("a_pts_ff")))
           
    df = df.withColumn("score_margin", col("h_pts_ff") - col("a_pts_ff"))
    
    # 4. Event Parsing
    df = df.withColumn("assister", regexp_extract(col("desc"), r"\((.*?)\s+\d+\s+AST\)", 1))
    
    # 5. Clutch Time Flag
    df = df.withColumn(
        "is_clutch",
        when((col("period") >= 4) & (col("seconds_remaining") <= 300) & (col("score_margin") >= -5) & (col("score_margin") <= 5), 1)
        .otherwise(0)
    )
    
    return df
