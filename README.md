# Walmart End-to-End Modern Data Engineering Pipeline

An end-to-end modern data engineering project built using **Ghost DB (Identity Database), Databricks, dbt Core, Apache Airflow, Docker, and AWS S3**.

The project demonstrates a complete data pipeline starting from source data ingestion and CDC processing, followed by Bronze, Silver, and Gold transformations, incremental processing, SCD Type 2, dimensional modeling, and end-to-end workflow orchestration using Airflow.

---

## Architecture

```text
                         ┌─────────────────────┐
                         │   Ghost DB /        │
                         │   Identity Database  │
                         │    (PostgreSQL)      │
                         └──────────┬──────────┘
                                    │
                              CDC / Upsert
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │     Databricks      │
                         │    Data Ingestion   │
                         └──────────┬──────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │   Bronze Layer      │
                         │   Staging Layer     │
                         └──────────┬──────────┘
                                    │
                                    │
                         ┌──────────▼──────────┐
                         │       dbt Core      │
                         │  Databricks Adapter │
                         └──────────┬──────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │  Silver Technical   │
                         │      Models         │
                         └──────────┬──────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │  Silver Business    │
                         │      Models         │
                         └──────────┬──────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │    Gold Layer       │
                         │                     │
                         │  OBT + Dimensions   │
                         │      + Facts        │
                         └──────────┬──────────┘
                                    │
                                    ▼
                            Business Analytics


             ┌────────────────────────────────────┐
             │          Apache Airflow             │
             │                                    │
             │  End-to-End Workflow Orchestration │
             └────────────────────────────────────┘


                    AWS S3
                      │
              New Files / Reviews
                      │
                      ▼
                Databricks
                      │
                      ▼
              Streaming Table
```

---

## Tech Stack

| Technology | Purpose |
|---|---|
| Ghost DB / Identity DB | Source / operational database |
| PostgreSQL | Source database technology |
| Databricks | Data ingestion, processing and data platform |
| dbt Core | Data transformation and modeling |
| Databricks Adapter | Connects dbt Core with Databricks |
| PySpark | Data processing and transformation |
| Apache Airflow | Workflow orchestration |
| Docker | Containerized Airflow environment |
| AWS S3 | File-based data ingestion |
| Git / GitHub | Version control |

---

# Pipeline Overview

The project is divided into multiple stages:

1. Source data in Ghost DB / Identity DB
2. CDC-based ingestion into Databricks
3. Bronze staging layer
4. dbt-based Silver transformations
5. Business transformations
6. Incremental processing
7. SCD Type 2 implementation
8. Dimensional modeling
9. Gold layer / OBT
10. Airflow orchestration
11. S3-based streaming ingestion

---

# 1. Source Database – Ghost DB

The source data is maintained in the **Ghost DB / Identity Database**.

The database acts as the source system for the pipeline.

The source contains the required Walmart-related data that is later ingested into Databricks for transformation and analytical processing.

---

# 2. CDC-Based Data Ingestion

Databricks is used to ingest data from the source database.

The ingestion process captures source data and loads it into the Databricks Bronze layer.

The ingestion process supports incremental data movement using CDC / upsert-based processing so that newly available or changed records can be processed without rebuilding the entire dataset.

The ingestion pipeline is configured with the required source database connection details and target Databricks objects.

---

# 3. Bronze Layer

A dedicated **Walmart catalog** is created in Databricks.

Within the catalog, a Bronze schema is maintained.

```text
Walmart Catalog
      │
      └── Bronze Schema
```

The Bronze layer acts as the **staging / raw ingestion layer**.

The purpose of this layer is to maintain the ingested source data before applying business transformations.

No major business transformations are applied at this stage.

---

# 4. dbt Core

After the Bronze layer is created, **dbt Core** is used for the transformation and modeling layer.

The dbt project is configured to connect to Databricks using the **dbt Databricks adapter**.

The dbt project contains multiple transformation layers.

```text
Bronze
   │
   ▼
Silver Technical
   │
   ▼
Silver Business
   │
   ▼
Gold
```

---

# 5. Source Definitions and Testing

The dbt project contains a `source.yml` configuration to define and document the source datasets.

Properties and testing configurations are also maintained for the transformation models.

These tests help validate the quality and consistency of the transformed data.

Examples include checks around:

- Unique keys
- Null values
- Referential relationships
- Model-level data quality

---

# 6. Silver Technical Layer

The **Silver Technical** layer contains technical transformations applied to the Bronze data.

The purpose of this layer is to clean, standardize and prepare the data for downstream business transformations.

Typical processing includes:

- Data type handling
- Standardization
- Filtering
- Technical transformations
- Record-level processing
- Incremental processing

---

# 7. Silver Business Layer

The **Silver Business** layer applies business-specific transformations and logic.

This layer converts technically processed data into structures that are easier to use for analytical and business requirements.

A business-oriented **One Big Table (OBT)** is also created as part of the transformation process.

The OBT combines the required business attributes and measures into a consolidated analytical structure.

---

# 8. Incremental Processing

dbt incremental materialization is used for applicable models.

Instead of rebuilding complete tables during every execution, incremental logic processes only the required new or changed records.

Example:

```text
Initial Load
     │
     ▼
Complete Dataset
     │
     ▼
Subsequent Runs
     │
     ▼
Only New / Changed Records
```

This reduces unnecessary processing and improves pipeline efficiency.

---

# 9. SCD Type 2

Slowly Changing Dimension Type 2 is implemented using **dbt snapshots**.

The snapshot configuration uses the required attributes such as:

- `unique_key`
- Snapshot strategy
- `updated_at`
- `dbt_valid_to`
- `dbt_valid_from`

This allows historical changes to dimension records to be maintained rather than overwriting previous versions.

Conceptually:

```text
Customer ID | Name       | Valid From | Valid To
---------------------------------------------------
101         | Rohit      | 2025-01-01 | 2026-03-10
101         | Rohit P.   | 2026-03-10 | NULL
```

This allows the historical state of a dimension to be preserved.

---

# 10. Gold Layer

The Gold layer contains the final analytical models required for business consumption.

The Gold layer includes:

- Dimensional models
- Fact tables
- Dimension tables
- OBT / analytical tables

The Gold layer provides a structured representation of the transformed data for downstream analytical use cases.

---

# 11. Dimensional Modeling

Dimensional modeling is implemented using dbt.

The transformation flow uses intermediate models and **ephemeral models** where appropriate.

The final dimensional model contains fact and dimension structures.

```text
Silver Business
       │
       ▼
Intermediate / Ephemeral Models
       │
       ├──────────────┐
       ▼              ▼
 Dimensions        Fact Tables
       │              │
       └───────┬──────┘
               ▼
          Gold Layer
```

Ephemeral models are used for intermediate transformation logic that does not need to be persisted as physical tables.

---

# 12. One Big Table (OBT)

An **One Big Table (OBT)** is created for business-oriented analytical requirements.

The OBT combines relevant business attributes and measures into a consolidated structure.

This provides a convenient dataset for downstream analytical and reporting use cases.

Metadata-driven processing is also incorporated into the OBT transformation logic.

---

# 13. Apache Airflow Orchestration

Apache Airflow is used to orchestrate the end-to-end pipeline.

Airflow is deployed using **Docker**.

The Airflow DAG contains Python-based tasks that define the execution sequence and dependencies between different stages of the pipeline.

The workflow automates the processing that would otherwise need to be executed manually.

The overall orchestration flow is:

```text
Trigger Data Ingestion
          │
          ▼
Wait for Ingestion Completion
          │
          ▼
Run dbt Transformations
          │
          ▼
Silver Technical
          │
          ▼
Silver Business
          │
          ▼
Gold / Dimensional Models
          │
          ▼
Final Analytical Layer
```

---

# 14. Airflow and Databricks Integration

Airflow is integrated with Databricks to trigger Databricks jobs programmatically.

The Databricks Workspace Client is used to:

1. Trigger the required Databricks job
2. Receive the job run information
3. Monitor the job execution status
4. Wait for completion
5. Continue to the next dependent Airflow task

A polling mechanism is implemented using a loop to continuously check the Databricks job status.

The next task is executed only after the required upstream job completes successfully.

```text
Airflow
   │
   │ Trigger
   ▼
Databricks Job
   │
   │ Monitor
   ▼
Job Status
   │
   ├── Running → Continue Checking
   │
   ├── Success → Next Task
   │
   └── Failed  → Pipeline Failure
```

This creates dependency-aware orchestration between Airflow and Databricks.

---

# 15. Scheduled Pipeline

The Airflow DAG is configured with a scheduled execution using cron syntax.

This allows the pipeline to execute automatically according to the defined schedule instead of requiring manual execution.

---

# 16. AWS S3 Streaming Ingestion

A separate ingestion pattern is implemented for file-based data arriving in **AWS S3**.

For example, customer review data can arrive as new files in an S3 bucket.

Databricks is configured with a streaming pipeline that monitors the S3 location.

```text
AWS S3
  │
  │ New Files
  ▼
Databricks Streaming Pipeline
  │
  ▼
Streaming Table
```

Whenever new data arrives in the configured S3 location, the Databricks streaming pipeline can process the newly available data into the streaming table.

This demonstrates a separate ingestion pattern for continuously arriving file-based data.

---

# End-to-End Workflow

The complete project can be summarized as:

```text
                    Ghost DB
                  Identity DB
                       │
                       │ CDC / Upsert
                       ▼
                 Databricks
                       │
                       ▼
                Bronze Layer
                (Staging)
                       │
                       ▼
                  dbt Core
                       │
                       ▼
             Silver Technical
                       │
                       ▼
              Silver Business
                       │
                       ▼
                    OBT
                       │
                       ▼
          Dimensional Modeling
             ┌────────┴────────┐
             ▼                 ▼
        Dimensions          Fact Tables
             │                 │
             └────────┬────────┘
                      ▼
                  Gold Layer
                      │
                      ▼
             Business Analytics


              Apache Airflow
                    │
                    ├── Trigger Ingestion
                    ├── Monitor Jobs
                    ├── Run dbt
                    └── Manage Dependencies


                  AWS S3
                    │
                    ▼
          Databricks Streaming
                    │
                    ▼
              Streaming Table
```

---

# Key Data Engineering Concepts Demonstrated

- CDC-based data ingestion
- Upsert processing
- Bronze / Silver / Gold architecture
- Databricks data ingestion
- dbt Core
- dbt Databricks adapter
- Incremental materialization
- dbt snapshots
- Slowly Changing Dimension Type 2
- Dimensional modeling
- Fact and dimension tables
- Ephemeral dbt models
- One Big Table (OBT)
- Metadata-driven transformation
- Data quality testing
- Apache Airflow DAGs
- Airflow task dependencies
- Airflow and Databricks integration
- Databricks job monitoring
- AWS S3 ingestion
- Streaming tables
- Dockerized Airflow
- Git and GitHub

# Key Takeaways

This project demonstrates a complete modern data engineering workflow by combining:

**Source → Ingestion → Bronze → Transformation → Silver → Dimensional Modeling → Gold → Orchestration → Analytics**

It also demonstrates how different processing patterns can coexist within a modern data platform, including:

- Batch / scheduled processing
- Incremental processing
- CDC / upsert-based ingestion
- SCD Type 2
- File-based streaming ingestion
- Workflow orchestration

The project was built to understand and demonstrate how **Databricks, dbt, Airflow, and cloud storage can work together to build an end-to-end modern data engineering pipeline.**
