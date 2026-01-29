# Staff Data Engineer Task: Design Documentation

## 1. Design Summary (TLDR)
**Architecture**: Layered (Staging/Intermediate/Marts) using **DuckDB** for local portability and **dbt** for transformations. (*Production Target: Snowflake*).

* **Staging**: `stg_shipment_events` standardizes raw event data.
* **Intermediate**: `int_shipment_events` and `int_orders` handle deduplication and window functions.
* **Marts**: `orders` provides an analytics-ready, single-row-per-order grain.

### Core Strategies
* **Deduplication**: Uses `row_number()` over `event_id` ordered by `ingest_at` to identify the "Latest Truth" and handle out-of-order events.
* **Late-Arriving Data**: Business timestamp `ingested_at` determines the most recent record.
* **Schema Evolution**: Resilient via explicit casting in Staging and `on_schema_change='append_new_columns'` for incremental models.
* **Idempotency**: Fully re-runnable; incremental logic uses a 3-day look-back buffer for safety.
* **Reliability**: Enforced via `data_tests` (uniqueness/null) and `accepted_values` validation on carriers and statuses.

---

## 2. Layered Architecture Principles

### **Staging Layer**
* **Purpose**: 1:1 representation of source data; the foundation for all downstream models.
* **Rules**: Only layer using the `source` macro. No joins or complex aggregations.
* **Operations**: Renaming, type casting, basic categorization, and converting units (e.g., cents to dollars).
* **Materialization**: Views.

### **Intermediate Layer**
* **Purpose**: The "heavy lifting" layer for complex transformations and enrichment.
* **Rules**: Uses `ref` macro only. Not exposed to end-users.
* **Operations**: Joins, deduplication, and complex computations.
* **Materialization**: Views by default; **Incremental Tables** for high-volume event data.

### **Marts Layer**
* **Purpose**: Conformed, business-ready entities (e.g., Orders, Couriers).
* **Rules**: Exposed to production schemas for BI and reporting. Denormalized and wide.
* **Operations**: Final metrics and KPIs. Avoid deduplication here (should be handled in Intermediate).
* **Materialization**: Tables or Incremental models.

---

## 3. Standards & Implementation

### **Model Materialization**
By default, models are **views**. For performance optimization (especially in Snowflake), we utilize:
* **Tables**: For models rebuilt entirely each run.
* **Incremental**: For large datasets to reduce compute by only processing new data.

### **File & Project Structure**
```text
models
├── staging/      # Raw cleaning & casting
├── intermediate/ # Joins & deduplication
└── marts/        # Business-facing entities
```

### **Documentation & YAML Requirements**
All models must include descriptions via doc blocks (markdown files) to ensure scalability.
* **Minimum Tests**: `unique` and `not_null` on primary keys.
* **Schema Metadata**: Explicit `data_type` definitions for observability.
* **Exposures**: Document downstream dependencies (dashboards, ML models) to track impact.

---

## 4. Validation & Operations
* **Testing**: Validated via `dbt build`, which runs models and tests sequentially.
* **Observability**: Integrated via YAML descriptions and dbt-generated documentation.
* **Trade-offs**: Chose batch processing for this task; however, the idempotent logic allows for near-live execution (every 1-5 mins) via a scheduler like Airflow or dbt Cloud.