# Retail Customer 360 Data Platform

## Project Overview
This project is an end-to-end Azure Data Engineering platform built using the Medallion Architecture (Bronze, Silver, Gold) to process, transform, and analyze retail customer data.

The solution ingests raw retail datasets into Azure Data Lake Storage Gen2, performs scalable data transformation and validation using Azure Databricks and PySpark, orchestrates workflows using Azure Data Factory, and delivers analytics-ready datasets for Power BI reporting and business intelligence.

The platform is designed using production-style data engineering practices including incremental ingestion, watermarking, Delta Lake processing, metadata-driven orchestration, rejected-record handling, and dimensional modeling.

---

## Architecture

### Cloud & Data Engineering Technologies
- Azure Data Factory (ADF)
- Azure Data Lake Storage Gen2 (ADLS Gen2)
- Azure Databricks
- Power BI
- Azure SQL Database
- Azure Key Vault

### Processing & Storage
- PySpark
- Delta Lake
- Parquet
- Delta Tables

### Data Engineering Concepts
- Medallion Architecture
- Incremental Data Loading
- Watermarking
- Metadata-Driven Pipelines
- Slowly Changing Dimensions (SCD)
- Data Validation
- Deduplication
- Error Handling
- Rejected Record Framework
- Orchestration Pipelines
- Dimensional Modeling
- Fact & Dimension Tables

---

## Medallion Architecture

### Bronze Layer
- Raw source data ingestion
- Incremental loading from landing zone
- Metadata-driven ingestion framework
- Storage in Delta/Parquet format
- Historical raw data preservation

### Silver Layer
- Data cleansing and standardization
- Null handling and schema validation
- Deduplication using window functions
- Rejected record identification and storage
- Business rule transformations

### Gold Layer
- Business-ready curated datasets
- Star schema dimensional modeling
- Fact and dimension table creation
- Aggregated analytical datasets
- Reporting and dashboard consumption layer

---

## Features

- Incremental ingestion pipelines
- Watermark-based processing
- Metadata-driven orchestration
- Delta Lake merge operations
- Rejected data handling framework
- Parameterized ADF pipelines
- Scalable PySpark transformations
- Dimensional data modeling
- Analytics-ready Gold layer datasets
- Power BI reporting integration

---

## Project Structure

```text
adf/            -> Azure Data Factory pipelines and orchestration
databricks/     -> PySpark notebooks and transformation logic
architecture/   -> Solution architecture and workflow diagrams
powerbi/        -> Dashboard files and reporting assets
screenshots/    -> Pipeline, notebook, and dashboard screenshots
documentation/           -> Additional project documentation
```

## Architecture Diagram

![Architecture Diagram](architecture/architecture.jpg)

## Project Documentation

[Download Full Project Documentation](docs/retail_customer_360_project_documentation.pdf)
