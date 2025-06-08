import json
from pyspark.sql.functions import col, lit, avg, sum as sum_, max as max_, to_date, trunc
from db_config import get_spark_session, read_table, write_table
from data_quality import check_nulls, check_row_count
from logger import get_logger

#load config
with open("/Users/vyomkeshharshwardhan/Downloads/DATAENG_PROJECT/etl_mini_project/config/etl_config.json") as config_file:
    config = json.load(config_file)

logger = get_logger(config["log_path"])

spark = get_spark_session()

#load tables
place_df = read_table(spark, config["tables"] ["place_master"])
weather_df = read_table(spark, config["tables"]["weather_readings"])
flood_df = read_table(spark, config["tables"]["flood_alerts"])


logger.info("Source Tables Loaded")

#data quality checks
check_nulls(place_df, ["place_id", "place_name"], logger)
check_row_count(weather_df, 1, logger)

# Filter for current month
month_date = config["month_date"]
print(month_date)
# Aggregate data
weather_agg = weather_df.groupBy("place_id").agg(
    avg("temperature").alias("avg_temperature"),
    sum_("rainfall").alias("total_rainfall"),
    avg("humidity").alias("avg_humidity")
)
weather_agg.show(5)

flood_agg = flood_df.groupBy("place_id").agg(
    max_("water_level").alias("max_water_level"),
    max_("flood_risk_level").alias("flood_risk_level")
)

# Join with place_master
summary_df = place_df.join(weather_agg, on="place_id", how="left") \
                     .join(flood_agg, on="place_id", how="left") \
                     .select(
                         col("place_id"),
                         col("place_name"),
                         col("avg_temperature"),
                         col("total_rainfall"),
                         col("avg_humidity"),
                         col("max_water_level"),
                         col("flood_risk_level")
                     ) \
                     .withColumn("month_date", trunc(to_date(lit(month_date)), "month")) \
                     .withColumn("load_status_id", lit(config["load_status_id"]))

# Load final summary table
write_table(summary_df, config["tables"]["target_summary"])
logger.info("Summary table written successfully.")

spark.stop()
logger.info("ETL Job completed successfully.")