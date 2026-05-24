# Enterprise Retail Analytics Pipeline (Customer 360)

## Project Overview
[cite_start]This project delivers a production-grade, end-to-end cloud data engineering platform that unifies siloed retail datasets—including customers, products, stores, inventory, and sales—into a centralized, analytics-ready Customer 360 platform using a modular Medallion Architecture[cite: 7, 9, 28]. 

[cite_start]The platform implements automated batch-based data ingestion into Azure Data Lake Storage Gen2, executes scalable distributed transformations and data validation via Azure Databricks, and handles modular workflow coordination using Azure Data Factory[cite: 35, 39, 47, 49]. [cite_start]The final reporting layer unifies data into an optimized star-schema model for executive business intelligence dashboards in Power BI[cite: 41, 42]. [cite_start]Built using production-style software engineering standards, the pipeline guarantees absolute reliability through watermark-based incremental loading, strict pipeline idempotency, and a structured Rejected Record Framework[cite: 11].

---

## Architecture & Technology Stack

### Cloud Platform & Core Tools
* [cite_start]**Data Orchestration:** Azure Data Factory (ADF) managing end-to-end control flows, dynamic execution parameters, and automated task sequencing[cite: 49].
* [cite_start]**Scalable Storage:** Azure Data Lake Storage Gen2 (ADLS Gen2) serving as the highly scalable primary data lake distributed across hierarchical storage boundaries[cite: 47].
* [cite_start]**Distributed Compute:** Azure Databricks running optimized distributed compute clusters to execute high-performance PySpark DataFrame transformations[cite: 47, 50].
* [cite_start]**Metadata & Audit Layer:** Centralized relational database infrastructure storing stateful control tables, file-tracking assets, and operational pipeline execution records[cite: 60, 69].
* [cite_start]**Business Intelligence:** Power BI for native analytical reporting, semantic star-schema data modeling, and data-driven executive visualization dashboards[cite: 42, 134].

### Core Engineering Specifications
* [cite_start]**Storage Protocols:** Optimized Parquet configurations and Delta Lake formats to support full transaction isolation, strict schema enforcement, and relational consistency[cite: 37, 47].
* [cite_start]**Architectural Pattern:** Medallion Architecture data refinement flow (Raw $\rightarrow$ Bronze $\rightarrow$ Silver $\rightarrow$ Gold)[cite: 16].
* [cite_start]**Design Frameworks:** High-efficiency watermark-based incremental loading logic [cite: 11][cite_start], rigorous pipeline idempotency execution routines [cite: 17][cite_start], and stateful metadata-driven operational control tracking[cite: 67].

---

## Medallion Architecture & Implementation Details

### 1. Ingestion Layer (Raw & Bronze)
* [cite_start]**Raw Ingestion:** Extracts raw enterprise data from source environments into cloud landing storage, retaining unaltered structure to protect data ancestry and downstream auditing integrity[cite: 35, 36].
* [cite_start]**Bronze Standardization:** Restructures landing datasets into a unified format within the Bronze layer to provide a standardized foundational starting point for high-performance spark operations[cite: 37].
* [cite_start]**Metadata Processing:** Evaluates active file configurations against centralized system control tables to successfully extract and load only newly updated or modified business data[cite: 38, 69].

### 2. Validation & Quality Layer (Silver)
* [cite_start]**Schema Enforcement:** Applies programmatic data quality assertions to validate required key parameters, structural types, and handle empty fields[cite: 143, 148, 152].
* [cite_start]**Text Standardization:** Executes regular expression string parsing, leading/trailing whitespace trimming, and column character formatting using PySpark[cite: 148, 149].
* [cite_start]**Data Deduplication:** Eliminates duplicate data variants utilizing high-performance distributed windowing functions to ensure a single accurate entry across datasets[cite: 146].
* [cite_start]**Quarantine Framework:** Isolates malformed schema entries, bad dates, and incorrect variables away from cleanly validated records, moving them into automated rejected-data paths to protect processing integrity[cite: 40, 144].

### 3. Analytics Layer (Gold)
* [cite_start]**Dimensional Modeling:** Converts validated business records into highly performant dimensional structures (`dim_customers`, `dim_products`, `dim_stores`) linked cleanly to core business metrics (`fact_sales`)[cite: 41, 120].
* [cite_start]**Idempotent Merges:** Coordinates atomic Delta Lake merge statements to process records via secure upsert configurations, seamlessly updating existing data changes while inserting new entities without data duplication[cite: 52, 154].
* [cite_start]**Analytical Serving:** Curates production-ready reporting sets focused on optimizing real-time key metrics, tracking sales trends, assessing store profitability, and summarizing regional metrics[cite: 41, 360].

---

## Technical Features & Project Capabilities

* [cite_start]**Stateful Metadata Ingestion:** Coordinates pipeline tasks dynamically using persistent control tables to monitor last-run tracking marks, operational statuses, and partition steps[cite: 69].
* [cite_start]**Watermark Incremental Processing:** Shields distributed engines from expensive duplicate loops by verifying incoming transaction markers against historical timestamps to stream only fresh modifications[cite: 72, 324].
* [cite_start]**Production Error Resilience:** Mitigates pipeline execution disruptions by incorporating graceful fault paths for no-data delivery periods, preserving smooth control runs[cite: 44].
* [cite_start]**Unified Audit Tracking:** Logs explicit pipeline run IDs, schema layer paths, row manipulation metrics, and precise run-time measurements inside automated system tables to streamline production debugging workflows[cite: 111, 112].

---

## Repository Structure

```text
├── adf/                # Production Azure Data Factory pipeline definitions and parameters
├── notebooks/          # Modular PySpark notebooks containing cleansing, quality, and logic layers
├── database/           # Master database DDL scripts for tracking, audit logs, and file schemas
├── architecture/       # Detailed end-to-end solution architecture diagrams and data lineage maps
├── powerbi/            # Star-schema analytical models and dashboard visualization designs
└── documentation/      # Full technical solution specifications and edge-case testing logs
