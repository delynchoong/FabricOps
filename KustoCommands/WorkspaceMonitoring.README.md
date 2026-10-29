# Workspace monitoring: common Eventhouse investigations

Use [WorkspaceMonitoring.kql](WorkspaceMonitoring.kql) for **read-only**
investigation of Fabric Eventhouse ingestion, query performance, capacity
throttling, scale-out reasons, and reduced sizing. Sources and schemas were
checked against public Microsoft documentation on **2026-10-29**.

This is a troubleshooting guide, not an administrative deployment script. It
does not enable monitoring, change policies, resize capacity, cancel queries, or
modify data. For deliberate tuning changes, see
[Optimizations.README.md](Optimizations.README.md). Return to the
[command guides index](README.md) for both workflows.

## 1. Prerequisites and query context

1. A workspace admin configures workspace monitoring and **turns on collection**.
   Creating the Monitoring Item alone does not mean collection has started.
2. Open the source workspace's **monitoring KQL database** from the Monitoring
   Item. Collection can be centralized in another monitoring environment; each
   source workspace has its own monitoring database. Do not assume one selected
   database automatically queries every source workspace.
3. Use an account with permission to query that database. The overview documents
   workspace users with at least Contributor access; explicitly shared database
   access may also apply. Check your organization's sharing and role policies.
4. Run section **0** of the KQL file to discover tables and inspect schemas.
   Supported tables may not exist until matching item activity produces logs.
   If a table is absent, skip its dependent query and investigate collection,
   product support, permissions, and source activity. Do not interpret that as
   zero failures or healthy operation.
5. Run one numbered query at a time, **including its own `let` declarations**.
   The examples use independent parameters so they can be copied into a Queryset.
   Empty ID filters include all visible items in the selected database; set
   `targetWorkspaceId` and `targetEventhouseId` to discovered `WorkspaceId` and
   Eventhouse `ItemId` strings when narrowing scope. Names are display labels,
   not stable identifiers.
6. Adjust the bounded `lookback` for the incident. All timestamps are UTC. For
   exact correlation across separate queries, replace relative windows with the
   same fixed incident start/end timestamps.

The monitoring database is read-only. Section **20** is different: its `.show`
commands must run against the **application Eventhouse endpoint/database** to
inspect currently running activity. Running them on the monitoring Eventhouse
would inspect the monitoring service, not the production workload.

Sources:
[workspace monitoring overview](https://learn.microsoft.com/en-us/fabric/fundamentals/workspace-monitoring-overview),
[configure collection and access](https://learn.microsoft.com/en-us/fabric/fundamentals/enable-workspace-monitoring),
[Eventhouse monitoring](https://learn.microsoft.com/en-us/fabric/real-time-intelligence/monitor-eventhouse).

## 2. Tables and what they actually tell you

| Table | Use | Important interpretation |
| --- | --- | --- |
| `EventhouseMetrics` | Ingestion volume/latency/queues, batch size, materialized views, continuous exports | Not a generic live node CPU, RAM, CU utilization, or active-query counter. Metric units and supported aggregations vary by `MetricName`. |
| `EventhouseQueryLogs` | Recorded query duration, CPU, peak memory, cache misses, application, failures | Historical query records, not a live running-query inventory. Query text and identity fields can contain sensitive operational information. |
| `EventhouseCommandLogs` | Costly command types, applications, and completion statuses | Commands can compete with querying; CPU time is not billed CUs. |
| `EventhouseDataOperationsLogs` | Background `BatchIngest`, `UpdatePolicy`, `MaterializedView`, and `RowStoreSeal` activity | A source row can participate in multiple operation kinds; summing their rows/bytes is not unique source ingestion volume. |
| `EventhouseIngestionResultsLogs` | Queued-ingestion statuses, errors, retry guidance, update-policy attribution | Covers queued ingestion, not all streaming outcomes. Log records are not original source event counts. |
| `EventhouseCapacityThrottlingState` | Recorded capacity throttling states and exits | The monitoring overview describes updates around every 30 seconds to one minute. Count records as observations, not distinct incidents. |
| `EventhouseScaleoutEvents` | Why an Eventhouse scaled out | Reasons are aggregated and reported every **24 hours**. A record can contain multiple reasons; it is not one instantaneous resize or a node-count history. |
| `EventhouseSubOptimalSizeAfterThrottling` | Evidence that the Eventhouse remains undersized after throttling | A heartbeat is reported every **five minutes** while reduced sizing continues. If the engine is unavailable, these events are not reported. |

Monitoring starts after collection is enabled, with **no historical backfill**.
The documented default retention is 30 days; actual retention can be changed.
Telemetry ingestion is delayed and can itself be affected by resource issues.
An empty result means **no matching evidence was returned**, not proof of health.

## 3. Quick investigation map

Numbers below match the numbered sections in
[WorkspaceMonitoring.kql](WorkspaceMonitoring.kql).

| Symptom | Run | Inspect | Next step |
| --- | --- | --- | --- |
| No monitoring data or an apparently stale dashboard | **0-2** | Available tables, item IDs, metric names, last-seen timestamps, raw dimensions | Check collection is on, correct source database, permissions, retention, and whether the source produced relevant activity. Some metrics are activity-driven rather than continuous. |
| New data arrives late | **3-8**, then **9-12** | Ingested bytes, batching/discovery/ingestion latency, queue age, failures, capacity restrictions | Separate upstream discovery delay, deliberate batching delay, processing backlog, and ingestion errors before changing settings. |
| 429s, rejected queries, or intermittent query access | **9-10**, **15**, then **20** | Capacity state, exact failure text, application admission limits | Distinguish capacity throttling from workload-group limits and client retry storms. Keep correlation IDs and UTC times. |
| Performance remains poor after throttling exited | **10**, **12-18** | Exit observation, recent reduced-size heartbeats, competing query/command/background activity | Check current Eventhouse sizing and Capacity Metrics. A throttle exit does not prove the Eventhouse returned to optimal size. |
| Unexpected scale-out or higher compute usage | **11**, **3**, **13-14**, **17-19** | Daily reason summary, ingestion load, expensive applications and operations | Correlate over the reported 24-hour period, not the exact record timestamp. Review minimum-capacity scheduling and actual capacity telemetry. |
| Slow queries or high CPU/memory consumption | **13-18**, then **20** | p95/p99 duration, CPU-heavy queries, scan sizes, cache misses, command/background competition | Investigate query shape and workload mix; CPU time summed across workers is not wall-clock CPU percentage or billed CU utilization. |
| Materialized views or continuous exports lag | **19**, **18**, **9-12**, then **20** | Raw/latest metric values and dimensions, operation CPU, throttling, current view state | Inspect view enablement/materialization progress and export errors. Do not raise global concurrency until the bottleneck is understood. |

## 4. Metrics: choose the correct aggregation

`EventhouseMetrics` contains preaggregated observations, including
`MetricCount`, `MetricSumValue`, `MetricMinValue`, `MetricMaxValue`, and a dynamic
`MetricSpecificDimensions` bag. First inspect section **1** or **2** rather than
assuming every metric has a database/table dimension.

- **Counter/volume metrics:** section **3** sums `MetricSumValue` for
  `IngestionVolumeInBytes`, retaining database/table identity. The output uses
  GiB (`bytes / 1024^3`), not decimal GB.
- **Average metrics:** sections **4-6** calculate
  `sum(MetricSumValue) / sum(MetricCount)`. Do not average already-averaged
  records without weighting by sample count. Zero samples produce an explicitly
  null average, not a false zero.
- **Queue gauges:** use average queue length or oldest-message age over time.
  Summing a queue gauge across time does not measure total ingested messages.
  Queue components can use different units of work: batching queues use blob
  messages, while ingestion queues use batches. Do not add them together.
- **Keep dimensions:** section **4** retains component identity and ingestion
  kind where available. Discovery latency is not included in the documented
  ingestion/stage latency measurements; do not casually add independent metric
  averages and call the sum an exact end-to-end percentile.
- **Materialized-view/export gauges and results:** section **19** preserves
  the latest sample per metric and dimension bag. `Result` dimensions carry
  outcome meaning for result metrics; a numeric value of 1 alone is not proof
  of success. Keep observations and sample ages visible.
- **Metric spelling:** `ContinousExportPendingCount` is the spelling currently
  published in the metric reference; do not silently correct it to a different
  metric name. Confirm which names actually arrive using section **1**.
- **Units:** follow the metric reference's Unit column. It lists seconds for
  `MaterializedViewAgeSeconds` and minutes for continuous-export lateness in
  its description, but some descriptions are inconsistent. Section **19**
  leaves values raw rather than applying an unverified conversion or threshold.

Reference:
[EventhouseMetrics schema, metric names, dimensions, and aggregation types](https://learn.microsoft.com/en-us/fabric/real-time-intelligence/monitor-metrics).

## 5. Throttling: identify the layer before changing limits

### Capacity state observations

Sections **9-10** use the documented `ThrottlingState` values:

| State | Meaning for investigation |
| --- | --- |
| `Overage Protection` | Future capacity is being consumed within the protection range; the documented state has no user impact yet. |
| `Interactive Delay` | Responses can slow and Eventhouse autoscale is disabled to reduce CU consumption. |
| `Interactive Rejection` | Queries are throttled; the Eventhouse might scale in. |
| `Background Rejection` | All operations are throttled and the Eventhouse is reduced to minimum size. |
| `Exited Throttling` | A recorded exit, not proof that optimal sizing or end-to-end performance has already returned. |

The latest observation is **not guaranteed current state**, especially if it is
old. No record within seven days means the query cannot determine state. For
duration analysis, include the last state before the incident window, account
for repeated observations, and mark unknown/unobserved intervals. Do not
multiply the number of records by the nominal reporting cadence to infer exact
incident duration.

### Request-level admission

Section **15** preserves `FailureReason` instead of classifying every rejection
as capacity exhaustion. Failures identifying `RequestRateLimitPolicy` or a
workload group call for inspecting that group's concurrency/rate policy and
enforcement scope using section **20**. These are separate from Fabric
capacity-wide bursting, smoothing, and throttling.

Use bounded retries with backoff and jitter; do not repeatedly flood a throttled
endpoint. Review the Fabric Capacity Metrics app with your capacity admin for
actual utilization, smoothing, and other workspaces sharing the same capacity.
This KQL file cannot attribute all capacity consumption across other workloads.

References:
[Eventhouse capacity throttling logs](https://learn.microsoft.com/en-us/fabric/real-time-intelligence/monitor-logs-capacity-throttling),
[Fabric throttling](https://learn.microsoft.com/en-us/fabric/enterprise/throttling),
[workload-group rate limits](https://learn.microsoft.com/en-us/kusto/management/request-rate-limit-policy?view=microsoft-fabric).

## 6. Scale-outs and reduced sizing

Section **11** exposes `ScaleoutReason` as the **concatenated reason summary**
reported for the preceding 24 hours. Do not split it into supposed independent
resize events or use record count to measure how many times the Eventhouse
scaled. Examples of documented reason categories include CPU, memory, cache,
ingestion load, and configured minimum consumption.

The summary does not contain a per-event new/old node count or exact scale
transition timestamp. For an incident lasting minutes, use Eventhouse System
overview, capacity controls, Capacity Metrics, and relevant workload telemetry
alongside the daily summary. Correlation is a lead, not proof of causation.

Section **12** shows first/last observed reduced-size heartbeats inside the
selected lookback. They are **not exact start/end times**. An Eventhouse can
remain below optimal size after exiting throttling until available capacity or
load changes. Missing heartbeats do not prove recovery: the engine may be
unavailable, collection delayed, or the window/database wrong.

This guide does not pause/restart capacity or change minimum consumption.
Those are capacity-admin actions with cost and availability implications.

References:
[scale-out events and reporting cadence](https://learn.microsoft.com/en-us/fabric/real-time-intelligence/monitor-logs-scaleout-events),
[sub-optimal size after throttling](https://learn.microsoft.com/en-us/fabric/real-time-intelligence/monitor-logs-sub-optimal-size),
[Eventhouse System overview](https://learn.microsoft.com/en-us/fabric/real-time-intelligence/manage-monitor-eventhouse),
[minimum capacity scheduling](https://learn.microsoft.com/en-us/fabric/real-time-intelligence/eventhouse-smart-capacity-control).

## 7. Query, ingestion, and background-operation details

- **Query outcomes:** section **13** groups by observed `Status`, not an assumed
  universal success value. The official samples use `Failed` and `Throttled`
  for query error analysis. Section **15** also retains nonempty failure text.
- **Completed versus active:** logged query duration is historical. To inspect
  activity currently executing, switch to the application endpoint for section
  **20**. Permissions restrict which users' operations you can see.
- **Heavy queries:** section **14** ranks query CPU and includes memory/scan
  counts and a bounded query preview. A low row count can still have high CPU.
  Do not share query text, identities, or correlation data outside approved
  incident channels; even a preview can contain sensitive literals.
- **Hot-cache misses:** section **16** reports a byte-weighted ratio over
  observed hot hits/misses, not cache occupancy or query-results cache hit rate.
  A null ratio means no measured bytes in the selected sample, not perfect cache
  performance. Review cache windows against actual query ranges before expanding
  cache or restricting the query's data scope.
- **Ingestion results:** sections **7-8** describe queued-ingestion results.
  Retries and update policies complicate counts; do not equate result records
  with original rows, unique source files, or a full streaming failure rate.
- **Commands/background:** sections **17-18** separate command CPU from data
  operations. Avoid adding CPU values from different logging layers and claiming
  a non-overlapping total without verifying accounting semantics.

Schema references:
[query logs](https://learn.microsoft.com/en-us/fabric/real-time-intelligence/monitor-logs-query),
[command logs](https://learn.microsoft.com/en-us/fabric/real-time-intelligence/monitor-logs-command),
[data-operation logs](https://learn.microsoft.com/en-us/fabric/real-time-intelligence/monitor-logs-data-operation),
[queued-ingestion results](https://learn.microsoft.com/en-us/fabric/real-time-intelligence/monitor-logs-ingestion-results).

## 8. Incident checklist

1. Record the UTC incident window, source workspace, Eventhouse `ItemId`, capacity
   ID, affected database, client application, symptoms, and available correlation
   IDs. Do not include secrets or unrestricted query text in exported notes.
2. Confirm database context, collection state, available tables, and data
   freshness before interpreting an empty result.
3. Check throttle states and reduced-size observations, then inspect query,
   command, ingestion, and background load in the same window.
4. Use scale-out summaries as daily context, not precise incident timestamps.
5. Compare the incident against a known-good period with similar traffic. Query
   latency, ingestion freshness, correctness, and capacity cost all matter.
6. Use live `.show` commands on the application endpoint only where historical
   monitoring cannot answer the question.
7. Escalate with evidence to the workspace/capacity owner. Apply any tuning
   separately through the [optimization guide](Optimizations.README.md), one
   change at a time, with an explicit rollback and validation plan.

## Public sources and validation boundary

All table/field and metric names used here are grounded in the public schema
references linked above. Additional examples are available in Microsoft's
[workspace-monitoring samples](https://github.com/microsoft/fabric-samples/tree/main/workspace-monitoring).
Live command visibility is documented in
[show running queries](https://learn.microsoft.com/en-us/kusto/management/show-queries-command?view=microsoft-fabric),
[show workload groups](https://learn.microsoft.com/en-us/kusto/management/show-workload-group-command?view=microsoft-fabric),
[request classification](https://learn.microsoft.com/en-us/kusto/management/show-cluster-policy-request-classification-command?view=microsoft-fabric),
[materialized views](https://learn.microsoft.com/en-us/kusto/management/materialized-views/materialized-view-show-command?view=microsoft-fabric),
and [operations](https://learn.microsoft.com/en-us/kusto/management/show-operations?view=microsoft-fabric).

These are schema-based investigation templates. They have not been executed
against your monitoring database, so actual table availability, data arrival,
permissions, and query results still require validation in your environment.
