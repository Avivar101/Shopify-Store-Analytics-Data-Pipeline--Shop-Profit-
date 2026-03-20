# Shopify Analytics Platform

A **containerized, production-style analytics system** for ecommerce data, built using a modern analytics engineering stack.

This project demonstrates how to design and implement a complete analytics workflow—from ingestion to dashboards—using cost-efficient and modular tools.



## Overview

This system ingests Shopify data, transforms it into structured analytics models, and delivers business insights through dashboards.

It is designed to reflect real-world analytics architecture, including:

* Incremental data ingestion
* Structured transformation layers (dbt)
* Data quality testing
* Orchestration (Dagster)
* BI dashboards (Metabase)
* Containerized deployment (Docker)



## Architecture

```text
Shopify API
    ↓
Python Ingestion (incremental, watermark-based)
    ↓
DuckDB (raw, append-only storage)
    ↓
dbt (staging → dimensions → facts → marts)
    ↓
Metabase (dashboards)
    ↓
Dagster (orchestration)
```
<div align="center">
  <img src="assets/shopify architecture 1x.png" width="700" alt="Data Pipeline">
  <p><em>Figure 1: The Data Pipeline Architecture</em></p>
</div>



## Key Features

### Incremental Ingestion

* Pulls Shopify data using `updated_at_min`
* Handles pagination
* Watermark-based tracking via `pipeline_state`
* Append-only raw tables with batch metadata

### Data Modeling (dbt)

* Layered approach:

  * `stg_*` (cleaning + deduplication)
  * dimensions (`dim_*`)
  * facts (`fct_*`)
  * marts (business-facing models)
* Dimensional modeling:

  * order vs line-item grain separation
  * product vs variant separation
* SCD Type 2 implemented for product history

### Data Quality

* Schema tests: `not_null`, `unique`, `relationships`
* Business logic validation
* Test execution integrated into pipeline

### Orchestration

* Dagster pipeline:

  ```text
  ingestion → dbt run → dbt test
  ```
* Environment-aware execution
* CLI-based dbt integration

### Storage

* DuckDB (embedded OLAP)
* File-based persistence with Docker volume

### BI Layer

* Metabase dashboards
* Supports internal reporting and embedded analytics use cases

### Containerization

* Fully Dockerized setup
* Reproducible environment using Docker Compose
* Volume-based persistence



## Tech Stack

* **Data Ingestion:** Python
* **Warehouse:** DuckDB
* **Transformation:** dbt
* **Orchestration:** Dagster
* **BI:** Metabase
* **Infrastructure:** Docker, Docker Compose



## Project Structure

```text
.
├── ingestion/              # Python ingestion scripts
├── dbt_transform/         # dbt models (staging, marts, etc.)
├── dagster_project/       # Dagster pipeline definitions
├── db/                    # DuckDB database (mounted volume)
├── metabase_plugins/      # Metabase plugin directory
├── docker-compose.yml
├── Dockerfile
└── README.md
```


## Getting Started

### Prerequisites

* Docker
* Docker Compose



### 1. Clone the Repository

```bash
git clone https://github.com/your-username/shopify-analytics-platform.git
cd shopify-analytics-platform
```



### 2. Configure Environment

Create a `.env` file:

```env
DUCKDB_PATH=/app/db/shopify.duckdb
SHOPIFY_API_KEY=your_key
SHOPIFY_API_PASSWORD=your_password
SHOPIFY_STORE_URL=your_store_url
```



### 3. Run the System

```bash
docker compose up --build
```



### 4. Access Services

* Dagster UI: [http://localhost:3000](http://localhost:3000)
* Metabase: [http://localhost:3001](http://localhost:3001)



## Workflow

The pipeline runs in the following order:

1. **Ingestion**

   * Fetches new/updated data from Shopify
   * Stores in raw DuckDB tables

2. **Transformation (dbt)**

   * Cleans and structures data
   * Builds dimensions and fact tables
   * Creates analytics-ready models

3. **Testing**

   * Validates data quality and integrity

4. **Delivery**

   * Data is exposed via dashboards in Metabase



## Example Use Cases

* Ecommerce performance tracking (revenue, AOV, LTV)
* Customer behavior analysis
* Product performance analytics
* Multi-store data unification
* Embedded analytics for SaaS platforms



## Design Decisions

### Why DuckDB?

* Lightweight and cost-efficient
* Ideal for embedded analytics and local development
* Zero infrastructure overhead

### Why dbt?

* Structured transformation workflow
* Reusable, testable models
* Industry-standard analytics engineering tool

### Why Dagster?

* Fine-grained orchestration
* Clear pipeline structure
* Better observability than basic schedulers

### Why Docker?

* Reproducible environment
* Eliminates local setup inconsistencies
* Enables future deployment flexibility



## Trade-offs

* DuckDB is not ideal for high-concurrency or multi-user production environments
* Metabase plugin-based DuckDB support requires careful version management
* Current setup prioritizes cost-efficiency and modularity over scale



## Current Status

### Completed

* End-to-end pipeline (ingestion → transformation → orchestration)
* Containerized environment
* Data modeling (facts, dimensions)
* Incremental ingestion system

### In Progress

* Business metrics (analytics marts)
* Dashboard refinement



## Future Improvements

* Replace DuckDB with scalable warehouse (BigQuery/Snowflake)
* Add observability and logging layer
* Implement alerting for pipeline failures
* Introduce semantic layer for metrics consistency
* Enable production deployment (cloud environment)



## Positioning

This project is not just a pipeline.

It represents a **modular analytics system** with:

* ingestion layer
* transformation layer
* orchestration layer
* delivery layer

Designed to reflect real-world analytics engineering practices for:

* internal reporting
* SaaS analytics platforms
* embedded analytics use cases



## Author

Built by Okezie Ben-John

I design and build analytics systems and data products using modern data stack tools.



## License

MIT License (or your preferred license)



## 📞Contact

You can connect with me and contact me for collaboration and gigs via:

[![YouTube](https://img.shields.io/badge/YouTube-FF0000?style=for-the-badge&logo=youtube&logoColor=white)](https://www.youtube.com/@okezieben722)

[![WhatsApp](https://img.shields.io/badge/WhatsApp-25D366?style=for-the-badge&logo=whatsapp&logoColor=white)](https://wa.me/2349118122768)

[![LinkedIn](https://img.shields.io/badge/LinkedIn-0A66C2?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/okeziebenj/)
