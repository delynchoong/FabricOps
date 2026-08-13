---
name: nyc-taxi-medallion-demo
description: Reproduce or demonstrate the verified NYC Taxi Bronze-Silver-Gold architecture in Microsoft Fabric, including notebooks, orchestration, Direct Lake, and Power BI validation.
---

# NYC Taxi Medallion Demo

Use this skill when the user asks to reproduce, explain, demo, or extend the
NYC Taxi medallion solution.

## Verified reference deployment

- Workspace: `<workspace-name>`
- Capacity: `<capacity-name>` (`F2`, `<region>`)
- Lakehouse: `nyc_taxi_lakehouse`
- Pipeline: `NYC_Taxi_Medallion_Pipeline`
- Semantic model: `NYC Taxi Analytics`
- Report: `NYC Taxi Workshop`

Resource IDs and connection details are intentionally excluded. Discover
resources by display name when reproducing the demo.

## Mandatory operating rules

1. Invoke the `e2e-medallion-architecture` skill before making Fabric changes.
2. Add `x-ms-fabric-skill: e2e-medallion-architecture` to every
   `api.fabric.microsoft.com` request, including LRO and job-status polls.
3. Use one schema-enabled lakehouse with `bronze`, `silver`, and `gold` schemas.
4. Never copy or print bearer tokens. Acquire tokens silently from an encrypted
   cache or use an approved interactive login.
5. Discover workspace and item IDs by display name before creating resources.
6. Make all deployment operations idempotent: update matching items instead of
   creating duplicates.
7. Do not stop after authoring notebooks. Execute Bronze, Silver, and Gold,
   validate tables, execute the pipeline, then validate Direct Lake with DAX.

## Source data

Use the January 2024 NYC TLC yellow taxi Parquet file and taxi-zone CSV.
Land both files in OneLake before Spark reads them:

```text
Files/landing/nyc_taxi/yellow_tripdata_2024-01.parquet
Files/landing/nyc_taxi/taxi_zone_lookup.csv
```

Do not read public HTTP URLs directly from Fabric Spark.

## Deployment sequence

### 1. Discover or create the workspace

1. List Fabric workspaces.
2. Match `SKILLS_NYC_DEMO` by exact display name.
3. If absent, create it and assign it to the selected capacity.
4. Confirm capacity assignment before creating items.

### 2. Discover or create the schema-enabled lakehouse

Create or reuse `nyc_taxi_lakehouse`.

Bind all notebooks using this metadata shape:

```json
{
  "metadata": {
    "dependencies": {
      "lakehouse": {
        "default_lakehouse": "<lakehouse-id>",
        "default_lakehouse_name": "nyc_taxi_lakehouse",
        "default_lakehouse_workspace_id": "<workspace-id>"
      }
    },
    "kernelspec": {
      "display_name": "Synapse PySpark",
      "language": "Python",
      "name": "synapse_pyspark"
    },
    "language_info": {
      "name": "python"
    }
  },
  "nbformat": 4,
  "nbformat_minor": 5
}
```

Every code cell must include:

```json
{
  "execution_count": null,
  "outputs": []
}
```

### 3. Create the schemas

Use Fabric Livy/Spark SQL to create:

```sql
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
```

### 4. Bronze ingestion

Create notebook `01_bronze_ingest`.

Required behavior:

- Read the staged trip Parquet and zone CSV.
- Validate required source columns.
- Add `ingestion_timestamp`, `ingestion_date`, `source_file`,
  `source_system`, `batch_id`, and `source_month`.
- Replace only the January 2024 source-month partition on reruns.
- Write Delta tables:
  - `bronze.yellow_taxi_trips_raw`
  - `bronze.taxi_zones_raw`
- Fail if the trip count is zero or the zone count is below 250.

Expected verified result:

```text
BRONZE_TRIP_ROWS=2964624
```

### 5. Silver transformation

Create notebook `02_silver_transform`.

Required behavior:

- Convert source columns to snake_case and enforce data types.
- Deduplicate on pickup, drop-off, locations, distance, and total amount.
- Enforce:
  - non-null pickup/drop-off timestamps
  - pickup before drop-off
  - location IDs from 1 through 265
  - passenger count from 1 through 8
  - distance greater than 0 and at most 200 miles
  - duration from 1 through 360 minutes
  - fare from 0 through 1000
  - total amount from 0 through 2000
  - pickup dates in January 2024
- Enrich pickup and drop-off locations with borough and zone names.
- Generate a deterministic SHA-256 `trip_id`.
- Write:
  - `silver.yellow_taxi_trips`
  - `silver.taxi_zones`
  - `silver.data_quality_metrics`
- Partition trips by `pickup_date`.
- Run:

```sql
OPTIMIZE silver.yellow_taxi_trips
ZORDER BY (pickup_borough, pickup_hour);
```

Expected verified results:

```text
SILVER_VALID_ROWS=2713898
SILVER_REJECTED_ROWS=250726
```

### 6. Gold aggregation

Create notebook `03_gold_aggregate`.

Set before writes:

```python
spark.conf.set("spark.sql.parquet.vorder.default", "true")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.optimizeWrite.binSize", "1g")
```

Create:

- `gold.date`
- `gold.borough`
- `gold.nyc_taxi_activity`
- `gold.nyc_taxi_daily_summary`
- `gold.nyc_taxi_borough_hour_summary`

Aggregate by pickup date, pickup borough, and pickup hour. Include trips,
passengers, distance, duration, fare, tips, tolls, and total revenue.

Optimize:

```sql
OPTIMIZE gold.nyc_taxi_activity
ZORDER BY (date_key, borough_key, pickup_hour);

OPTIMIZE gold.nyc_taxi_daily_summary
ZORDER BY (date_key);

OPTIMIZE gold.nyc_taxi_borough_hour_summary
ZORDER BY (date_key, borough_key, pickup_hour);
```

Expected verified results:

```text
GOLD_ACTIVITY_ROWS=3810
GOLD_DAILY_ROWS=31
GOLD_TOTAL_TRIPS=2713898
```

### 7. Deploy and execute notebooks

For each notebook:

1. List notebook items and match the exact display name.
2. Create only when absent.
3. Base64-encode the `.ipynb`.
4. Call the notebook `updateDefinition` endpoint using `format: ipynb`.
5. Poll the LRO to `Succeeded`.
6. Check for a recent active notebook job to avoid duplicate submission.
7. Run with:

```json
{
  "executionData": {
    "parameters": {},
    "configuration": {
      "defaultLakehouse": {
        "id": "<lakehouse-id>",
        "name": "nyc_taxi_lakehouse"
      },
      "useStarterPool": true
    }
  }
}
```

Execute strictly in this order:

```text
01_bronze_ingest
02_silver_transform
03_gold_aggregate
```

Stop and diagnose the exact Spark error if a layer fails. Never retry an
unchanged failed definition.

### 8. Pipeline orchestration

Create or update `NYC_Taxi_Medallion_Pipeline` with:

```text
Bronze Ingestion
  -> Silver Transformation (after Bronze succeeds)
    -> Gold Aggregation (after Silver succeeds)
```

Each activity:

- type: `TridentNotebook`
- timeout: one hour
- retries: 2
- retry interval: 60 seconds
- session tag: `nyc-medallion`

Define string parameter `ProcessingDate`, defaulting to `2024-01-01`, and pass
it to each notebook as `processing_date`.

Run the pipeline once and require terminal status `Completed`.

Record the completed pipeline job ID outside source control when audit
traceability is required.

### 9. Direct Lake semantic model

Create `NYC Taxi Analytics` over the Gold lakehouse tables.

Model:

- Date dimension
- Borough dimension
- Taxi Activity fact
- Relationships from the fact to both dimensions
- Direct Lake storage mode

Include explicit measures for:

- Total Trips
- Total Revenue
- Average Fare
- Average Trip Distance
- Tip Rate

Trigger the initial Direct Lake refresh/reframe before running DAX.

Expected DAX results:

```text
Total Trips = 2713898
Total Revenue = 74.27M
Average Fare = 18.40
Average Trip Distance = 3.30
Tip Rate = 18.9%
```

### 10. Power BI report

Create one FHD page named `NYC Taxi Workshop` with:

- KPI cards
- daily trend
- borough ranking
- detailed activity table

Validate PBIR before publication. Require zero errors and zero warnings.
After publication, execute the DAX used by the trend, ranking, and detail
visuals to confirm live Direct Lake results.

## Final verification checklist

- Workspace is assigned to the intended capacity.
- Lakehouse is schema-enabled.
- All three schemas exist.
- All source files exist in the landing folder.
- All three notebooks are bound to the correct lakehouse.
- Bronze, Silver, and Gold notebook jobs completed.
- Expected table counts match or have an explained variance.
- Pipeline completed without altering expected row counts.
- Direct Lake model was initialized.
- DAX measures return live data.
- PBIR validation has zero errors and warnings.
- No token, cache, generated package manifest, or temporary dependency folder
  was written to the repository.

## Demo prompt

Use this prompt from the workspace:

```text
Use the nyc-taxi-medallion-demo skill to explain the verified architecture.
Show the Bronze, Silver, and Gold transformations, the pipeline dependencies,
the validation results, and the Direct Lake reporting path. Do not redeploy or
modify Fabric resources unless I explicitly ask.
```

To reproduce in another workspace:

```text
Use the nyc-taxi-medallion-demo skill to reproduce the solution in <workspace>
on <capacity>. Discover all IDs dynamically, show the plan before making
changes, and execute through pipeline, Direct Lake, DAX, and PBIR validation.
```
