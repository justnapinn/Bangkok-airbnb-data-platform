# Bangkok Airbnb Data Platform: End-to-End Data Pipeline

An end-to-end Data Engineering project designed to ingest, process, and analyze Airbnb data for Bangkok. This repository contains implementations for both an **AWS Cloud environment** and a fully functional **Local Simulation**.

## Repository Structure

This repository is organized into three main components:

### 1. `Local-Code/`
Contains the implementation of the pipeline running entirely on a local machine.
- **`batch_pipeline.ipynb`**: A Jupyter Notebook that performs Batch ETL using **PySpark** and loads data into a **DuckDB** Data Warehouse.
- **`streaming_pipeline.ipynb`**: A Python script simulating Real-time Streaming (Producer/Consumer) using Python Queues and **TinyDB** (NoSQL).

### 2. `process-data-script/` (Cloud ETL)
Contains PySpark scripts designed to run on **AWS Glue**.
- **`process_listings_hosts.py`**: Reads raw listings data from S3, cleans/transforms it, and splits it into `hosts` and `listings` Parquet files.
- **`process_calendar.py`**: Reads raw calendar data from S3, fixes date formats, cleans prices, and saves as Parquet files.

### 3. `create-db-redshift/` (Cloud DWH)
Contains SQL DDL commands to define the Star Schema in **Amazon Redshift**.
- **`dim_hosts.txt`**: SQL to create the Host dimension table.
- **`dim_listings.txt`**: SQL to create the Listing dimension table.
- **`fact_calendar.txt`**: SQL to create the Calendar fact table.

---

## Architecture Overview

The project demonstrates two architectural approaches to handle the Data Lifecycle (Ingest -> Transform -> Store -> Analyze):

### ☁️ Cloud Architecture (AWS)
* **Data Lake:** Amazon S3 (Raw & Processed Zones)
* **ETL Processing:** AWS Glue (Serverless PySpark)
* **Data Warehouse:** Amazon Redshift (Star Schema)
* **Orchestration:** Designed for Amazon MWAA (Airflow)

### 💻 Local Architecture (Simulation)
* **Data Lake:** Local File System
* **ETL Processing:** Local PySpark (Anaconda)
* **Data Warehouse:** DuckDB (OLAP Database)
* **Streaming:** Python Threading & Queue + TinyDB (JSON Document Store)

---

## How to Run

### Option A: Running Locally (Recommended)
**Prerequisites:** Python, Jupyter Notebook, PySpark, DuckDB, Pandas, TinyDB, ftfy.

1.  **Setup Data:** Ensure your raw Airbnb data (`.csv.gz`) is placed in a folder named `raw_data` inside your project directory.
2.  **Run Batch Pipeline:** Open `Local-Code/batch_pipeline.ipynb` and run all cells. This will process the CSVs and create a `airbnb_dw.duckdb` database.
3.  **Run Streaming Pipeline:** Open `Local-Code/streaming_pipeline.ipynb` and run all cells to simulate real-time review processing.

### Option B: Running on AWS
**Prerequisites:** AWS Account with access to S3, Glue, and Redshift.

1.  **S3:** Upload raw data to an S3 bucket (e.g., `s3://your-bucket/raw-listings/`).
2.  **Redshift:** Create a cluster and use the SQL scripts in `create-db-redshift/` to create the tables.
3.  **Glue:** Create ETL Jobs using the scripts in `process-data-script/`. Update the `args` (parameters) to point to your specific S3 buckets.
4.  **Run:** Trigger the Glue jobs manually or via a scheduler.

---

## Authors
* **Jakkapong Yamsang**
* **Jinnaphat Phansri**
* **Thapanee Sawangwiwat**

*Course: DE486 Data Engineering Integration, Srinakharinwirot University*
