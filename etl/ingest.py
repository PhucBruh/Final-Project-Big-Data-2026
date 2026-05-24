from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

def create_spark_session(app_name="NBA_BigData_Pipeline"):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

def get_raw_schema():
    return StructType([
        StructField("gameid", StringType(), True),
        StructField("period", IntegerType(), True),
        StructField("clock", StringType(), True),
        StructField("h_pts", DoubleType(), True),
        StructField("a_pts", DoubleType(), True),
        StructField("team", StringType(), True),
        StructField("playerid", StringType(), True),
        StructField("player", StringType(), True),
        StructField("type", StringType(), True),
        StructField("subtype", StringType(), True),
        StructField("result", StringType(), True),
        StructField("x", DoubleType(), True),
        StructField("y", DoubleType(), True),
        StructField("dist", DoubleType(), True),
        StructField("desc", StringType(), True),
        StructField("season", IntegerType(), True)
    ])

def ingest_data(spark, data_path="data/raw/*.csv"):
    schema = get_raw_schema()
    df = spark.read.csv(data_path, header=True, schema=schema)
    return df
