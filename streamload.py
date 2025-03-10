import snowflake.connector
import os
from dotenv import load_dotenv

# ✅ Load environment variables from .env file (Ensure you have credentials stored)
load_dotenv()

# ✅ Snowflake Connection Credentials
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_DATABASE = "TOMTOM_DB"
SNOWFLAKE_SCHEMA = "ANALYTICS_TOMTOM"
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"  # Change if needed

# ✅ Connect to Snowflake
conn = snowflake.connector.connect(
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    account=SNOWFLAKE_ACCOUNT,
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema=SNOWFLAKE_SCHEMA
)

cur = conn.cursor()

# ✅ Query to get new records from STREAMS
fetch_new_data_query = """
SELECT Rank_by_filter, World_rank, City, Average_travel_time_per_6_mi, Change_from_2023, Congestion_level_percent, Time_lost_per_year_at_rush_hours
FROM TOMTOM_DB.RAW_DATA_SCHEMA.METRO_AREA_STREAM
WHERE METADATA$ACTION = 'INSERT';  -- Only capture INSERT operations
"""

# ✅ Fetch new records
cur.execute(fetch_new_data_query)
new_records = cur.fetchall()

if new_records:
    print(f"✅ Found {new_records} new records! Inserting into final table...")

    insert_query = """
INSERT INTO TOMTOM_DB.PROCESSED_DATA_SCHEMA.METRO_AREA_FINAL 
(Rank_by_filter, World_rank, City, Average_travel_time_per_6_mi, Change_from_2023, Congestion_level_percent, Time_lost_per_year_at_rush_hours)
SELECT Rank_by_filter, World_rank, City, Average_travel_time_per_6_mi, Change_from_2023, Congestion_level_percent, Time_lost_per_year_at_rush_hours
FROM TOMTOM_DB.RAW_DATA_SCHEMA.METRO_AREA_STREAM
WHERE METADATA$ACTION = 'INSERT';
"""

    cur.execute(insert_query)
    
    print("✅ Data appended successfully to final table!")

else:
    print("⚠️ No new records found in the stream. Skipping insert.")

# ✅ Close connection
cur.close()
conn.close()
print("🔒 Connection closed.")
