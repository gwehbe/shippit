# Staff Data Engineer Exercise 

This repository contains a thin-slice implementation of a reliable data pipeline built with **dbt** and **DuckDB**. It demonstrates how to transform raw, high-frequency shipment lifecycle event data into an analytics-ready representation of order and shipment states.

## Mock Data
The project includes a python script that was used to generate a mock, 100+ record dataset with some duplication and late-arriving event data. The dataset generated was saved to `seeds/shipment_lifecycle_events.csv`.

The following command runs the python script to generate the dataset (optional step, since the dataset file is already in the repo):
```bash
python3 generate_seeds.py
```

## 🚀 Quick Start (Local Execution)

Follow these steps to run the pipeline without any cloud infrastructure.

### 1. Prerequisites
* Python 3.8+
* [DuckDB](https://duckdb.org/) (handled automatically via dbt adapter)

### 2. Environment Setup
```bash
# Create and activate a virtual environment
python3 -m venv venv
source venv/bin/activate

# Install the dbt-duckdb adapter
pip install dbt-duckdb
```

### 3. Load Environment Variables
The project is configured to be portable by using a local `profiles.yml` file kept inside the project folder. By default, dbt won't find it because it looks in `~/.dbt/`, so a file named `.env` has been created in the root directory that sets `DBT_PROFILES_DIR` to make the repo portable.

You can use the provided `.env` file or run the commands directly:
```bash
# Load environment variables
export DBT_PROFILES_DIR=$(pwd)
```

### 4. Run the Pipeline
```bash
# Execute the full build
dbt build

# Alternatively, execute each step individually
dbt deps
dbt seed
dbt run
dbt test
```
