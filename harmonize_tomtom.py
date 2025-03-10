from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, regexp_replace, to_decimal, to_timestamp
import os
from dotenv import load_dotenv

# ✅ Load environment variables from .env file (Ensure credentials are stored)
load_dotenv()

# ✅ Snowflake Connection Credentials
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_DATABASE = "TOMTOM_DB"
SNOWFLAKE_SCHEMA_RAW = "RAW_DATA_SCHEMA"
SNOWFLAKE_SCHEMA_HARMONIZED = "HARMONIZED_TOMTOM"
SNOWFLAKE_WAREHOUSE = "HOL_WH"

# ✅ Establish Snowflake Connection
connection_parameters = {
    "account": SNOWFLAKE_ACCOUNT,
    "user": SNOWFLAKE_USER,
    "password": SNOWFLAKE_PASSWORD,
    "database": SNOWFLAKE_DATABASE,
    "schema": SNOWFLAKE_SCHEMA_RAW,
    "warehouse": "COMPUTE_WH"  # ✅ Use quotes around the warehouse name

}

session = Session.builder.configs(connection_parameters).create()

# ✅ Load raw data from Metro Area table
raw_data = session.table(f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA_RAW}.METRO_AREA_TABLE")

# ✅ Data Transformation
harmonized_data = (
    raw_data
    .withColumn("average_travel_time_minutes", regexp_replace(col("Average_travel_time_per_6_mi"), " min.*", "").cast("INTEGER"))  # Extract travel time in minutes
    .withColumn("congestion_level_percent", regexp_replace(col("Congestion_level_percent"), "%", "").cast("FLOAT"))  # Convert congestion to float
    .withColumn("time_lost_hours", regexp_replace(col("Time_lost_per_year_at_rush_hours"), " hours", "").cast("INTEGER"))  # Extract time lost in hours
    .drop("Average_travel_time_per_6_mi", "Congestion_level_percent", "Time_lost_per_year_at_rush_hours")  # Drop original columns
    .dropDuplicates(["City", "World_rank"])  # Remove duplicate records
)

# ✅ Create or Replace the Harmonized Table
harmonized_data.write.mode("overwrite").save_as_table(f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA_HARMONIZED}.METRO_AREA_HARMONIZED")

print("✅ Traffic data harmonization completed. Transformed data stored in METRO_AREA_HARMONIZED.")

# ✅ Close the session
session.close()
print("🔒 Snowflake session closed.")
