# BigData_Assignment_03
Traffic Congestion Index – from TomTom

# PROBLEM STATEMENT
The Traffic Congestion Index from TomTom offers valuable real-time insights into traffic conditions, such as congestion level changes and average travel time per 6 miles. However, efficiently obtaining, processing, and analyzing this data in an automated and scalable way presents several challenges, especially since the data needs to be updated daily. But our dataset has a static data which is why it cannot be incremented automatically.

# TECHNOLOGIES USED
**Snowflake:** Selected for its ability to manage large amounts of structured and semi-structured data in a scalable, cloud-based environment. Its integration with Snowpark allows for efficient data transformations directly within the data warehouse.
**Snowpark (Python):** Utilized for processing and transforming the data within Snowflake, focusing on data cleaning, aggregation, and analytics. Python offers flexibility to implement complex algorithms and metrics, such as calculating congestion levels and changes in traffic time.
**Snowflake Streams and Tasks:** Chosen to automate incremental updates and track changes in the dataset, allowing the daily data pipeline to run efficiently without reprocessing existing data.
**GitHub:** Used for version control, task management, and team collaboration. It helps maintain an organized workflow and facilitates smooth cooperation among team members.

# ARCHITECTURAL DIAGRAM
![My Image]("C:\Users\RMBJ\Downloads\Ass3_bigdata.drawio 1.png")

#  WALKTHROUGH OF THE APPLICATION

Step-by-Step Instructions:
**1. Data Ingestion**
Action: Configure Snowflake External Stages to ingest TomTom Traffic Congestion Data from external sources.
Details: The data is scraped daily but in our case, it will be updated on an yearly basis in CSV/JSON format and loaded into Snowflake External Stages for subsequent processing.
**Raw Data Storage**
Action:  Set up a schema to store raw traffic data.
Details: Raw traffic data is stored within Snowflake tables, making it easily accessible for transformation.
**Data Transformation**
Action: Utilize Snowpark Python for data transformation.
Details: Data cleaning, harmonization, and transformation logic are applied to convert raw data into a structured format.
**Incremental Data Updates**
Action: Set up Snowflake Streams and Tasks for automated incremental updates.
Details: Snowflake Streams track changes in the raw data, and Snowflake Tasks automate daily data updates.
**Analytics and Aggregation**
Action: Create precomputed analytics tables for daily/weekly performance metrics.
Details: Aggregations are performed on the transformed data, and the results are stored in a dedicated schema.
**2. Pipeline Orchestration**
The scheduling of tasks within the pipeline is managed directly in Snowflake.
**Key DAGs:**
****Data Ingestion:** **Snowflake's built-in task scheduling ensures that the data ingestion from the external stage occurs on a regular basis, with the raw traffic data being loaded into the staging area for further processing.
**Transformation:** Data transformation is handled through Snowflake Tasks that run the necessary transformations on the raw data and update the harmonized schema..
**Data Updates:** Snowflake Tasks are set up to process any bulk updates when new data is ingested, ensuring the system is always up-to-date with the most recent dataset.
**3. CI/CD Pipeline Integration**
The CI/CD pipeline ensures automated deployment and testing of the Snowpark Python code and Snowflake tasks. The integration includes:
**GitHub Actions for CI/CD:**
 A GitHub Actions workflow automates the deployment of Snowpark Python code to Snowflake. This includes running unit tests for user-defined functions (UDFs) and stored procedures before code deployment.
**The CI/CD process involves:**
Code Commit: Developers push changes to the GitHub repository.
CI/CD Trigger: A GitHub Actions workflow is triggered upon code changes.
Build & Test: The workflow builds the project and runs tests to ensure the integrity of Snowpark Python code and SQL transformations.
Deploy: After successful tests, the code is deployed to Snowflake environments (DEV, PROD) automatically.

# DIRECTORY STRUCTURE

Assignment3Part2
├── Assignment 3-Part 2 Spring 2025.pdf
├── README.md
├── Snowflake_notebook
├── harmonize.py
├── load_raw.py
├── scripts
├── setup_snowflake.sql
├── tom_extraction.py
├── transformation.py
├── udf_function
└── venv
# Contributions
Madhura Adadande: 33%
Ashwin Badamikar: 33%
Vemana Anilkumar: 33%
All team members actively participated in discussions, debugging sessions, and review meetings to ensure the successful completion of the project.
We, the members of this team, confirm that this submission is our original work and that we have accurately represented our contributions.






