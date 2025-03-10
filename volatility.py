import os
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, stddev, avg
import ace_tools as tools  # For displaying results

# ✅ Load environment variables (.env file should contain Snowflake credentials)
load_dotenv()

# ✅ Snowflake Connection Parameters from .env
connection_parameters = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "role": os.getenv("SNOWFLAKE_ROLE"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": "TOMTOM_DB",
    "schema": "Raw_Data_Schema"
}

# ✅ Create Snowflake Session
session = Session.builder.configs(connection_parameters).create()

# ✅ Define Function to Compute Volatility
def calculate_volatility(session, table_name):
    """
    Computes the volatility (standard deviation) of congestion levels for each city.
    
    Args:
        session: Snowpark session
        table_name (str): Snowflake table name (Metro or City Center)

    Returns:
        Pandas DataFrame with volatility results
    """
    df = (
        session.table(table_name)
        .select(col("CITY"), col("CONGESTION_LEVEL_PERCENT"))
        .group_by(col("CITY"))
        .agg(stddev(col("CONGESTION_LEVEL_PERCENT")).alias("VOLATILITY"))
    )

    return df.to_pandas()

# ✅ Compute Volatility for Metro Area
metro_volatility_df = calculate_volatility(session, "HARMONIZED_TOMTOM.METRO_AREA_HARMONIZED")

# ✅ Compute Volatility for City Center
city_volatility_df = calculate_volatility(session, "HARMONIZED_TOMTOM.CITY_CENTER_HARMONIZED")

# ✅ Display Results
tools.display_dataframe_to_user(name="Metro Area Volatility", dataframe=metro_volatility_df)
tools.display_dataframe_to_user(name="City Center Volatility", dataframe=city_volatility_df)

# ✅ Close Snowflake Session
session.close()
