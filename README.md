# BigData Assignment 03  
## Traffic Congestion Index – from TomTom

---

## 📝 Problem Statement

The **Traffic Congestion Index** from TomTom offers valuable real-time insights into traffic conditions, such as congestion level changes and average travel time per 6 miles.

However, efficiently obtaining, processing, and analyzing this data in an automated and scalable way presents several challenges — especially since the data is typically updated daily. In our case, the dataset is **static**, so it **cannot be incremented automatically**.

---
Assignment3Part2/
├── README.md                         # Project overview, instructions, architecture diagram
├── AiUseDisclosure.md               # Disclosure of any AI tools used
├── Assignment 3-Part 2 Spring 2025.pdf  # Original assignment spec
├── architecture/
│   └── traffic_pipeline_arch.png    # Architecture diagram (visual workflow)
│
├── snowflake_notebooks/             # Snowflake notebook scripts for Snowpark
│   ├── data_ingestion.ipynb         # Notebook to ingest TomTom data into Snowflake
│   └── analytics_exploration.ipynb  # Notebook for analytics on congestion index
│
├── snowpark_code/                   # Snowpark Python scripts
│   ├── load_raw.py                  # Load raw CSV/JSON data from S3
│   ├── harmonize.py                 # Harmonize and clean raw data
│   ├── transformation.py           # Data transformation logic
│   └── tom_extraction.py            # Data scraping logic (optional if scraped externally)
│
├── sql/
│   ├── setup_snowflake.sql         # Initial setup: roles, schemas, warehouses
│   └── analytics_queries.sql       # SQL for generating daily/weekly metrics
│
├── udfs/
│   ├── normalize_congestion.sql    # SQL UDFs for normalization logic
│   └── validate_data.py            # Python UDFs for data validation
│
├── scripts/
│   ├── dev_setup.jinja             # Jinja script for DEV environment
│   └── prod_setup.jinja            # Jinja script for PROD environment
│
├── tests/
│   ├── test_udfs.py                # Unit tests for UDFs
│   ├── test_transformations.py     # Unit tests for transformation logic
│   └── sample_data/                # Sample data for validation
│       └── traffic_sample.csv
│
├── .github/
│   └── workflows/
│       └── ci_cd.yml               # GitHub Actions workflow for CI/CD deployment
│
└── venv/                           

---

## 🚀 Technologies Used

- **Snowflake**  
  Selected for its ability to manage large amounts of structured and semi-structured data in a scalable, cloud-based environment. Its integration with **Snowpark** allows for efficient data transformations directly within the data warehouse.

- **Snowpark (Python)**  
  Used for processing and transforming data inside Snowflake. Python offers flexibility to implement complex algorithms and metrics, such as calculating congestion levels and changes in traffic time.

- **Snowflake Streams and Tasks**  
  Used to automate incremental updates and track changes in datasets. These enable an efficient pipeline without reprocessing existing data.

- **GitHub**  
  Employed for version control, task management, and team collaboration. GitHub helps maintain an organized workflow for smooth team cooperation.

---

## 🧱 Architectural Diagram

![Architecture_dia_3](https://github.com/user-attachments/assets/94bfb178-a6c6-4170-b077-6b8af56c5818)
)

<!-- Replace with the correct relative path or upload the image to GitHub -->

---

## 🔄 Walkthrough of the Application

### 1. 📥 Data Ingestion

- **Action**: Configure Snowflake External Stages to ingest TomTom traffic data.
- **Details**: The data is usually scraped daily. In this case, it's updated annually in **CSV/JSON format** and loaded into **Snowflake External Stages** for further processing.

---

### 2. 🧊 Raw Data Storage

- **Action**: Set up a schema to store raw traffic data.
- **Details**: Raw data is stored in Snowflake tables, making it easily accessible for transformation and analytics.

---

### 3. 🔄 Data Transformation

- **Action**: Use **Snowpark (Python)** for transformations.
- **Details**: Includes data cleaning, harmonization, and logic to convert raw input into a structured format.

---

### 4. 📈 Incremental Data Updates

- **Action**: Utilize **Snowflake Streams and Tasks** for automation.
- **Details**: Streams track changes, while Tasks handle daily update execution.

---

### 5. 📊 Analytics and Aggregation

- **Action**: Create precomputed tables for performance metrics.
- **Details**: Aggregations like daily/weekly congestion levels are stored in a dedicated schema.

---

### 6. ⚙️ Pipeline Orchestration

All scheduling is managed within **Snowflake** using native capabilities.

#### Key DAGs:
- **Data Ingestion**  
  Handled via Snowflake's task scheduling to load data into the staging area.

- **Transformation**  
  Snowflake Tasks run transformation logic on raw data, pushing results to the harmonized schema.

- **Data Updates**  
  Snowflake Tasks trigger reprocessing when new data arrives.

---

### 7. 🔁 CI/CD Pipeline Integration

A **CI/CD pipeline** ensures automated testing and deployment of Snowpark Python code and Snowflake Tasks.

#### GitHub Actions Integration:
- **Code Commit**: Developers push updates to GitHub.
- **CI/CD Trigger**: GitHub Actions starts a workflow.
- **Build & Test**: Project builds and tests run for UDFs and SQL logic.
- **Deploy**: Successfully tested code is deployed to **DEV** and **PROD** environments.

---




