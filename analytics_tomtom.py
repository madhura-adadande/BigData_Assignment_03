from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, avg, to_date, lag
from snowflake.snowpark.window import Window
import os
from dotenv import load_dotenv

# ✅ Load environment variables from .env file
load_dotenv()

# ✅ Snowflake Connection Parameters
connection_parameters = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "warehouse": "COMPUTE_WH",
    "database": "TOMTOM_DB",
    "schema": "HARMONIZED_TOMTOM"
}

# ✅ Establish Snowflake Session
session = Session.builder.configs(connection_parameters).create()

# ✅ Load Harmonized Traffic Data
harmonized_df = session.table("METRO_AREA_HARMONIZED")

# ✅ Convert 'REPORT_DATE' column to DATE type
harmonized_df = harmonized_df.with_column("REPORT_DATE", to_date(col("REPORT_DATE")))

# ✅ Compute Daily Congestion Level Metrics
daily_metrics = (
    harmonized_df
    .group_by("REPORT_DATE")
    .agg(avg("CONGESTION_LEVEL_PERCENT").alias("AVG_DAILY_CONGESTION"))
    .sort("REPORT_DATE")
)

# ✅ Compute Weekly Congestion Level Metrics
weekly_metrics = (
    harmonized_df
    .with_column("WEEK_NUMBER", col("REPORT_DATE").date_part("WEEK"))
    .group_by("WEEK_NUMBER")
    .agg(avg("CONGESTION_LEVEL_PERCENT").alias("AVG_WEEKLY_CONGESTION"))
    .sort("WEEK_NUMBER")
)

# ✅ Save Data to Analytics Schema
daily_metrics.write.mode("overwrite").save_as_table("ANALYTICS_TOMTOM.DAILY_TRAFFIC_METRICS")
weekly_metrics.write.mode("overwrite").save_as_table("ANALYTICS_TOMTOM.WEEKLY_TRAFFIC_METRICS")

print("✅ Analytics Computation Complete!")
print("🔢 Daily & Weekly Traffic Metrics Saved to Snowflake.")

# ✅ Close Snowflake Session
session.close()
print("🔒 Snowflake session closed.")
