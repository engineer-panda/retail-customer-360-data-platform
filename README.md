# Enterprise Retail Analytics Pipeline (Customer 360)

## Project Overview
This project delivers a production-grade, end-to-end cloud data engineering platform that unifies siloed retail datasets—including customers, products, stores, inventory, and sales—into a centralized, analytics-ready Customer 360 platform using a modular Medallion Architecture. 

The platform implements automated batch-based data ingestion into Azure Data Lake Storage Gen2, executes scalable distributed transformations and data validation via Azure Databricks, and handles modular workflow coordination using Azure Data Factory. The final reporting layer unifies data into an optimized star-schema model for executive business intelligence dashboards in Power BI. Built using production-style software engineering standards, the pipeline guarantees absolute reliability through watermark-based incremental loading, strict pipeline idempotency, and a structured Rejected Record Framework.

---

## Architecture & Technology Stack

### Cloud Platform & Core Tools
* **Data Orchestration:** Azure Data Factory (ADF) managing end-to-end control flows, dynamic execution parameters, and automated task sequencing.
* **Scalable Storage:** Azure Data Lake Storage Gen2 (ADLS Gen2) serving as the highly scalable primary data lake distributed across hierarchical storage boundaries.
* **Distributed Compute:** Azure Databricks running optimized distributed compute clusters to execute high-performance PySpark DataFrame transformations.
* **Metadata & Audit Layer:** Centralized relational database infrastructure storing stateful control tables, file-tracking assets, and operational pipeline execution records.
* **Business Intelligence:** Power BI for native analytical reporting, semantic star-schema data modeling, and data-driven executive visualization dashboards.

### Core Engineering Specifications
* **Storage Protocols:** Optimized Parquet configurations and Delta Lake formats to support full transaction isolation, strict schema enforcement, and relational consistency.
* **Architectural Pattern:** Medallion Architecture data refinement flow (Raw -> Bronze -> Silver -> Gold).
* **Design Frameworks:** High-efficiency watermark-based incremental loading logic, rigorous pipeline idempotency execution routines, and stateful metadata-driven operational control tracking.

---

## Medallion Architecture & Implementation Details

### 1. Ingestion Layer (Raw & Bronze)
* **Raw Ingestion:** Extracts raw enterprise data from source environments into cloud landing storage, retaining unaltered structure to protect data ancestry and downstream auditing integrity.
* **Bronze Standardization:** Restructures landing datasets into a unified format within the Bronze layer to provide a standardized foundational starting point for high-performance spark operations.
* **Metadata Processing:** Evaluates active file configurations against centralized system control tables to successfully extract and load only newly updated or modified business data.

### 2. Validation & Quality Layer (Silver)
* **Schema Enforcement:** Applies programmatic data quality assertions to validate required key parameters, structural types, and handle empty fields.
* **Text Standardization:** Executes regular expression string parsing, leading/trailing whitespace trimming, and column character formatting using PySpark.
* **Data Deduplication:** Eliminates duplicate data variants utilizing high-performance distributed windowing functions to ensure a single accurate entry across datasets.
* **Quarantine Framework:** Isolates malformed schema entries, bad dates, and incorrect variables away from cleanly validated records, moving them into automated rejected-data paths to protect processing integrity.

### 3. Analytics Layer (Gold)
* **Dimensional Modeling:** Converts validated business records into highly performant dimensional structures (`dim_customers`, `dim_products`, `dim_stores`) linked cleanly to core business metrics (`fact_sales`).
* **Idempotent Merges:** Coordinates atomic Delta Lake merge statements to process records via secure upsert configurations, seamlessly updating existing data changes while inserting new entities without data duplication.
* **Analytical Serving:** Curates production-ready reporting sets focused on optimizing real-time key metrics, tracking sales trends, assessing store profitability, and summarizing regional metrics.

---

## Technical Features & Project Capabilities

* **Stateful Metadata Ingestion:** Coordinates pipeline tasks dynamically using persistent control tables to monitor last-run tracking marks, operational statuses, and partition steps.
* **Watermark Incremental Processing:** Shields distributed engines from expensive duplicate loops by verifying incoming transaction markers against historical timestamps to stream only fresh modifications.
* **Production Error Resilience:** Mitigates pipeline execution disruptions by incorporating graceful fault paths for no-data delivery periods, preserving smooth control runs.
* **Unified Audit Tracking:** Logs explicit pipeline run IDs, schema layer paths, row manipulation metrics, and precise run-time measurements inside automated system tables to streamline production debugging workflows.

---

## Repository Structure

```text
├── adf/                # Production Azure Data Factory pipeline definitions and parameters
├── notebooks/          # Modular PySpark notebooks containing cleansing, quality, and logic layers
├── database/           # Master database DDL scripts for tracking, audit logs, and file schemas
├── architecture/       # Detailed end-to-end solution architecture diagrams and data lineage maps
├── powerbi/            # Star-schema analytical models and dashboard visualization designs
└── documentation/      # Full technical solution specifications and edge-case testing logs
