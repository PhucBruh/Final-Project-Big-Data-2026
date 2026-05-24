from ingest import create_spark_session, ingest_data
from clean_events import clean_events
from reconstruct_possessions import reconstruct_possessions, build_fact_possessions
from build_team_metrics import classify_shots, build_fact_shots, build_fact_team_metrics
from build_player_metrics import build_fact_player_metrics

def main():
    spark = create_spark_session()
    
    print("Ingesting Data...")
    df_raw = ingest_data(spark, "data/raw/*.csv")
    
    print("Cleaning Events...")
    df_clean = clean_events(df_raw)
    df_classified = classify_shots(df_clean)
    
    # Needs a checkpoint or cache here since multiple branches will use this
    df_classified.cache()
    
    print("Building Fact Shots...")
    fact_shots = build_fact_shots(df_classified)
    
    print("Reconstructing Possessions...")
    df_possessions = reconstruct_possessions(df_classified)
    fact_possessions = build_fact_possessions(df_possessions)
    
    # We also need fact_possessions to have gameid to get season
    fact_possessions.cache()
    
    print("Building Team Metrics...")
    fact_team_metrics = build_fact_team_metrics(df_classified, fact_possessions, fact_shots)
    
    print("Building Player Metrics...")
    fact_player_metrics = build_fact_player_metrics(df_classified, fact_shots)
    
    # Writing to Parquet
    print("Writing fact_shots to Parquet...")
    fact_shots.write.partitionBy("season").mode("overwrite").parquet("data/processed/fact_shots")
    
    print("Writing fact_possessions to Parquet...")
    # Need to add season to fact_possessions for partitioning
    game_season = df_classified.select("gameid", "season").dropDuplicates()
    fact_possessions_with_season = fact_possessions.join(game_season, "gameid", "left")
    fact_possessions_with_season.write.partitionBy("season").mode("overwrite").parquet("data/processed/fact_possessions")
    
    print("Writing fact_team_metrics to Parquet...")
    fact_team_metrics.write.partitionBy("season").mode("overwrite").parquet("data/processed/fact_team_metrics")
    
    print("Writing fact_player_metrics to Parquet...")
    fact_player_metrics.write.partitionBy("season").mode("overwrite").parquet("data/processed/fact_player_metrics")
    
    print("ETL Pipeline completed successfully!")

if __name__ == "__main__":
    main()
