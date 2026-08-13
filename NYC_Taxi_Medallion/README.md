# NYC Taxi Medallion Architecture

This project demonstrates a complete Bronze, Silver, and Gold data architecture
in Microsoft Fabric using January 2024 NYC TLC yellow taxi trip data.

The verified implementation includes:

- A schema-enabled Fabric Lakehouse
- PySpark notebooks for Bronze, Silver, and Gold processing
- A Fabric Data Pipeline for orchestration
- A Direct Lake semantic model
- A Power BI report

## Architecture

```text
NYC TLC source files
        |
        v
OneLake landing area
        |
        v
Bronze: raw ingestion and lineage metadata
        |
        v
Silver: validation, deduplication, typing, and zone enrichment
        |
        v
Gold: dimensional tables and reporting aggregates
        |
        v
Direct Lake semantic model
        |
        v
Power BI report
```

The solution uses one schema-enabled lakehouse with these schemas:

```text
bronze
silver
gold
```

## Source data

The implementation uses:

- January 2024 NYC TLC yellow taxi trip records in Parquet format
- NYC taxi zone lookup data in CSV format

Stage the files in OneLake before executing the notebooks:

```text
Files/landing/nyc_taxi/yellow_tripdata_2024-01.parquet
Files/landing/nyc_taxi/taxi_zone_lookup.csv
```

## Processing layers

### Bronze

Notebook: `01_bronze_ingest`

The Bronze layer:

- Reads the staged Parquet and CSV files.
- Validates required source columns.
- Adds ingestion timestamps, source metadata, batch ID, and source month.
- Replaces only the January 2024 source-month partition on reruns.
- Writes:
  - `bronze.yellow_taxi_trips_raw`
  - `bronze.taxi_zones_raw`

Verified trip count:

```text
2,964,624
```

### Silver

Notebook: `02_silver_transform`

The Silver layer:

- Converts columns to snake_case and enforces data types.
- Deduplicates trip records.
- Validates timestamps, location IDs, passenger counts, distance, duration,
  fare, and total amount.
- Restricts pickup dates to January 2024.
- Enriches pickup and drop-off locations with borough and zone names.
- Creates a deterministic SHA-256 trip ID.
- Writes:
  - `silver.yellow_taxi_trips`
  - `silver.taxi_zones`
  - `silver.data_quality_metrics`

Verified results:

```text
Valid trips:    2,713,898
Rejected trips:   250,726
```

### Gold

Notebook: `03_gold_aggregate`

The Gold layer creates:

- `gold.date`
- `gold.borough`
- `gold.nyc_taxi_activity`
- `gold.nyc_taxi_daily_summary`
- `gold.nyc_taxi_borough_hour_summary`

Aggregations cover trips, passengers, distance, duration, fares, tips, tolls,
and total revenue by date, borough, and pickup hour.

Verified results:

```text
Activity rows: 3,810
Daily rows:       31
Total trips:   2,713,898
```

## Pipeline orchestration

The `NYC_Taxi_Medallion_Pipeline` runs the notebooks in this order:

```text
Bronze Ingestion
  -> Silver Transformation
    -> Gold Aggregation
```

Each downstream activity starts only after its dependency succeeds. The
pipeline accepts a `ProcessingDate` parameter whose default is `2024-01-01`.

## Direct Lake and Power BI

The `NYC Taxi Analytics` semantic model uses Direct Lake over the Gold tables.
It includes Date and Borough dimensions related to the Taxi Activity fact.

Verified measures:

| Measure | Result |
| --- | ---: |
| Total Trips | 2,713,898 |
| Total Revenue | 74.27M |
| Average Fare | 18.40 |
| Average Trip Distance | 3.30 miles |
| Tip Rate | 18.9% |

The Power BI report contains KPI cards, a daily trend, borough ranking, and a
detailed activity table.

## Reproducing the solution

1. Create or select a Fabric workspace assigned to an active capacity.
2. Create a schema-enabled lakehouse.
3. Upload both source files to the documented OneLake landing paths.
4. Create the `bronze`, `silver`, and `gold` schemas.
5. Deploy and bind the three notebooks to the lakehouse.
6. Execute Bronze, Silver, and Gold in order.
7. Validate the table counts and data-quality metrics.
8. Create the pipeline and run it to completion.
9. Create and initialize the Direct Lake semantic model.
10. Validate the measures with DAX.
11. Publish and validate the Power BI report.

Workspace, lakehouse, notebook, pipeline, semantic-model, and report IDs are
tenant-specific. Always discover items by display name instead of copying IDs
from another deployment.

## Validation queries

Check the layer tables:

```sql
SHOW TABLES IN bronze;
SHOW TABLES IN silver;
SHOW TABLES IN gold;
```

Validate the core counts:

```sql
SELECT COUNT(*) FROM bronze.yellow_taxi_trips_raw;
SELECT COUNT(*) FROM silver.yellow_taxi_trips;
SELECT SUM(total_trips) FROM gold.nyc_taxi_daily_summary;
```

Expected values can vary if a different source month or revised NYC TLC source
file is used.

## Security

- Copy the repository `.env.example` to `.env` for local tenant and account
  configuration; the local file is excluded from Git.
- Use Microsoft Entra authentication.
- Never store bearer tokens or token caches in this repository.
- Prefer managed identity or a service principal for unattended deployments.
- Grant only the Fabric workspace and data permissions required by the
  deployment process.
