Order Lifecycle Pipeline Design
Architecture: Medallion (Bronze/Silver/Gold) using DuckDB for local portability and dbt for transformation.

Deduplication Strategy: Implemented a two-stage window function. The first stage handles event_id collisions at ingestion; the second stage identifies the "Latest Truth" per order_id based on business timestamps to handle out-of-order events.

Schema Evolution: By using select * in staging and explicit casting in the Marts layer, the pipeline is resilient to new upstream attributes while maintaining a strict contract for downstream consumers.

Trade-offs: * Batch vs. Stream: Chosen batch semantics for simplicity and local execution, though the logic is "streaming-ready" (idempotent).

State Management: Using ROW_NUMBER() is compute-heavy on massive datasets compared to incremental state tracking, but chosen here for absolute correctness and simplicity in a thin slice.




🏗 Architecture & Design
This solution implements a Medallion Architecture to ensure data reliability and observability:

Bronze (Seeds): Raw event data ingested from seed_order_lifecycle_events.csv.

Silver (Staging): stg_order_events handles deduplication and late-arriving data using window functions.

Gold (Marts): fct_orders provides a clean, single-row-per-order grain for downstream BI tools.

Key Engineering Decisions
Deduplication: Uses ROW_NUMBER() over event_id to handle exact duplicates or ingestion retries.

Late-Arriving Data: Uses business timestamps (event_timestamp) to determine the current state, ensuring that a "Shipped" status is never overwritten by a "Created" event that arrived late.

Idempotency: The pipeline is designed to be fully re-runnable. Using dbt build ensures that data integrity is verified by automated tests after every run.




🧪 Observability & Quality
Reliability is built into the code via dbt tests.

Uniqueness: The order_id in the final mart is tested to ensure no duplicates.

Integrity: Status fields are validated against a set of accepted_values.

Validation: You can view test results in the console output after running dbt build.

📁 Project Structure
/models: SQL transformation logic.

/seeds: Sample CSV data including edge cases (duplicates/late arrivals).

/tests: Data quality assertions.

profiles.yml: Local DuckDB configuration.