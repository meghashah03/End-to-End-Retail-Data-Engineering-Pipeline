# End-to-End Retail Lakehouse Data Engineering Platform

An end-to-end modern Lakehouse Data Engineering project that ingests retail data from **AWS S3** and **PostgreSQL**, processes it using the **Medallion Architecture (Bronze → Silver → Gold)** on **Databricks**, transforms data with **dbt**, orchestrates workflows using **Apache Airflow**, and builds an analytics-ready data warehouse using **Star Schema** and **Slowly Changing Dimensions (SCD Type 2)**.

---

## Project Overview

This project demonstrates the complete lifecycle of a production-grade data engineering pipeline.

It combines modern cloud technologies and industry-standard tools to build an automated, scalable, and analytics-ready Lakehouse platform capable of handling incremental data ingestion, transformation, validation, orchestration, and dimensional modeling.

---

## Tech Stack

| Category | Technologies |
|----------|--------------|
| Programming | Python, SQL |
| Processing Engine | Apache Spark (PySpark) |
| Lakehouse Platform | Databricks, Delta Lake |
| Data Transformation | dbt |
| Workflow Orchestration | Apache Airflow |
| Cloud Storage | AWS S3 |
| Database | PostgreSQL (Ghost AI Database) |
| Data Modeling | Star Schema, One Big Table (OBT), SCD Type 2 |
| Incremental Processing | Change Data Capture (CDC), Delta MERGE |
| Version Control | Git, GitHub |
| AI Integration | Ghost AI PostgreSQL, Model Context Protocol (MCP) |

---

# Solution Architecture

<img width="706" height="328" alt="Screenshot 2026-07-18 at 7 59 17 AM" src="https://github.com/user-attachments/assets/5f32fd04-951a-48da-9cd3-11c70284b66f" />


The pipeline follows the Medallion Architecture:

```
AWS S3
        \
         \
          --> Bronze --> Silver --> Gold
         /
PostgreSQL (CDC)

                ↓

            Delta Lake

                ↓

         dbt Transformations

                ↓

     Star Schema + OBT + SCD Type 2

                ↓

      Analytics Ready Data Warehouse

                ↓

      Apache Airflow Orchestration
```

---

# Key Features

## Data Ingestion

- Ingest retail datasets from AWS S3
- Capture incremental changes from PostgreSQL using CDC
- Store raw data in Delta Bronze tables

---

## Lakehouse Architecture

Implemented the complete **Bronze → Silver → Gold** architecture using Delta Lake.

### Bronze Layer

- Raw ingestion
- Schema preservation
- Incremental loads

### Silver Layer

- Data cleansing
- Standardization
- Business transformations
- Data quality checks

### Gold Layer

- Analytics-ready tables
- Star Schema
- Fact & Dimension tables
- Slowly Changing Dimensions (Type 2)

---

## Data Transformation using dbt

Implemented scalable dbt workflows including:

- Incremental Models
- Jinja Templating
- Data Quality Tests
- Ephemeral Models
- One Big Table (OBT)
- Star Schema
- SCD Type 2

---

## Workflow Orchestration

Apache Airflow automates the complete pipeline:

1. CDC ingestion
2. Source freshness validation
3. Silver Technical transformations
4. Technical data tests
5. Silver Business transformations
6. Business data tests
7. Gold Ephemeral models
8. Gold Dimensions
9. Gold Facts

---

## AI Database Integration

Integrated **Ghost AI PostgreSQL** with **Model Context Protocol (MCP)** to enable:

- AI-assisted database exploration
- Schema discovery
- Intelligent SQL generation
- Faster development workflow

---

# Project Workflow

```
AWS S3 + PostgreSQL
          │
          ▼
Incremental Ingestion
          │
          ▼
Bronze Layer
          │
          ▼
Silver Layer
          │
          ▼
dbt Transformations
          │
          ▼
Gold Layer
          │
          ▼
Star Schema
          │
          ▼
Analytics Ready Warehouse
```

---

# Screenshots

## Solution Architecture

<img width="706" height="328" alt="Screenshot 2026-07-18 at 7 59 17 AM" src="https://github.com/user-attachments/assets/8ac59f6f-c859-419c-af0d-236477efc72d" />


---

## Apache Airflow DAG

<img width="1716" height="328" alt="orchestrate-graph" src="https://github.com/user-attachments/assets/e45ff33e-8cee-4b6c-b9e9-8aa962ec3c9c" />


The entire pipeline is orchestrated using Apache Airflow.

Tasks include:

- CDC Ingestion
- Source Freshness
- Silver Transformations
- Data Quality Tests
- Gold Layer Generation

---

## Databricks Unity Catalog

<img width="1280" height="800" alt="Screenshot 2026-07-18 at 7 42 06 AM" src="https://github.com/user-attachments/assets/b161578f-34e3-4ae6-b5fc-67f8ea0038db" />


Organized data using:

- Bronze
- Silver
- Gold

schemas following the Medallion Architecture.

---

## Databricks Jobs

<img width="1280" height="800" alt="Screenshot 2026-07-18 at 7 45 39 AM" src="https://github.com/user-attachments/assets/75d94e5f-c759-4a27-8cf0-3516ab51ec0e" />
<img width="1280" height="800" alt="Screenshot 2026-07-18 at 7 44 20 AM" src="https://github.com/user-attachments/assets/815c7d7e-ed25-4a40-878b-e8377bffa80f" />


Automated ingestion jobs execute incremental pipelines into Delta Lake.

---

## Successful Pipeline Execution

<img width="1280" height="800" alt="Screenshot 2026-07-18 at 8 00 32 AM" src="https://github.com/user-attachments/assets/8f6e8b39-8e42-4e02-989f-3fea835b5aba" />


The pipeline completes successfully from ingestion through Gold layer creation.

---

## AWS S3

<img width="1280" height="800" alt="Screenshot 2026-07-18 at 7 46 42 AM" src="https://github.com/user-attachments/assets/f90be301-ebc5-4f90-89f8-e4510585f2a8" />


Amazon S3 acts as the landing zone for retail datasets.

---

# Repository Structure

```
.
├── airflow/
│   ├── dags/
│   └── docker-compose.yml
│
├── dbt/
│   ├── models/
│   │   ├── bronze/
│   │   ├── silver/
│   │   ├── gold/
│   │   └── tests/
│
├── notebooks/
│
├── scripts/
│
├── datasets/
│
├── architecture/
│
└── README.md
```

---

# Skills Demonstrated

- Data Engineering
- Apache Spark
- Databricks
- Delta Lake
- dbt
- Apache Airflow
- AWS S3
- PostgreSQL
- CDC Pipelines
- Incremental Data Processing
- Data Warehousing
- Star Schema
- Fact & Dimension Modeling
- Slowly Changing Dimensions (Type 2)
- Data Quality Testing
- ETL Pipeline Development
- Git & GitHub
- AI-assisted Database Exploration

---

# Future Improvements

- CI/CD using GitHub Actions
- Terraform Infrastructure as Code
- Great Expectations data validation
- Kafka-based streaming ingestion
- Real-time dashboards using Power BI
- Automated monitoring and alerting

---

# Author

**Megha Shah**

Master of Computer Applications  
National Institute of Technology Agartala

GitHub: https://github.com/meghashah03

LinkedIn: https://linkedin.com/in/megha-shah-020b92208
