# Eventhouse and Azure Data Explorer tuning commands

A practical command reference for improving **parallelism**, **concurrency**, and
**load efficiency** at the Eventhouse/cluster, database, and table levels.
Public Microsoft documentation and engineering blogs checked on **2026-10-29**.
Only commands with public Microsoft documentation are included.
These are examples, not settings applied to a live environment.

For the same command and query examples with one-line comments, open
[Optimizations.kql](Optimizations.kql). Run individual selections, not the entire file;
the examples include alternative settings and product-specific commands.

For read-only incident investigation, see [Workspace monitoring](WorkspaceMonitoring.README.md).
Return to the [command guides index](README.md) to choose a workflow.

## Read this first

- **Run one management command at a time**, in the correct database context.
  Do not execute this document as a script. Select an inspection command, review
  its result, then select an individual change.
- Replace `MyDatabase`, `Events`, `TenantId`, `Timestamp`, and other sample names
  with your own. Query examples assume `Events(Timestamp:datetime,
  TenantId:string, Value:real)`. Additional tables must already exist unless a
  creation command is explicitly shown.
- All numeric settings below are **illustrations, not recommended production
  defaults**. A higher limit can lower throughput by exhausting CPU, memory, or
  storage bandwidth. Some examples may lower your current limit.
- Save the complete `.show` output before changing anything. Prefer
  `.alter-merge` when documented: `.alter` can replace a policy or reset omitted
  properties. Arrays supplied to an alter-merge operation can replace the
  corresponding array; preserve entries you still need.
- Re-run each paired `.show` after the change, then measure behavior under the
  same workload. Successful policy readback alone does not prove improvement.
- For rollback, reapply the saved configuration using the documented command for
  that policy. If the original policy was inherited/null, restore inheritance
  using that policy's documented `.delete` command rather than pinning a copy of
  today's parent policy. Rollback cannot undo deleted data or completed rewrites.
- Use a test environment and least privilege. Retention changes can remove data;
  partitioning, backfill, and cache expansion can increase compute/storage cost.

### Product support and permissions

| Label | Meaning |
| --- | --- |
| **Both** | The cited command documentation applies to Fabric Eventhouse and ADX. |
| **ADX only** | The public command page applies to Azure Data Explorer; do not assume Fabric exposes it. |

`cluster` in supported Fabric commands can refer to the **Eventhouse**. It does
not imply access to every ADX cluster administration policy. Fabric capacity
management and ADX cluster scaling are separate control-plane operations.

Workload-group and request-classification administration requires
`Cluster AllDatabasesAdmin`; the Fabric `admin` role inherits this permission
according to the command documentation. ADX capacity and weak-consistency service
policies require `AllDatabasesAdmin`. Most table policies require Table Admin;
partitioning and materialized-view creation require Database Admin. Check the
linked command's permissions rather than treating Ingestor as sufficient.
Monitoring visibility is also permission-dependent.

### Public command and policy names

| Area | Publicly documented names |
| --- | --- |
| Ingestion | Use **`ingestionbatching`** for queued batching, **`streamingingestion`** for the ADX streaming policy, **`ingestiontime`** for ingestion timestamps, and **`capacity` / `IngestionCapacity`** for ADX operation slots. These solve different problems. |
| Workload groups | Management commands use **`workload_group`** for one group and **`workload_groups`** to list groups. |

## Choose the lever by symptom

| Symptom | Start here | What it improves | Scope / support |
| --- | --- | --- | --- |
| A large aggregation hits memory limits | [Shuffle and fanout](#1-parallelism-distribute-work-without-overloading-the-service) | Work distribution within one query | Request / Both |
| One application crowds out others | [Groups and classification](#2-concurrency-admission-control-and-workload-isolation) | Fairness and controlled simultaneous requests | Eventhouse/cluster / Both |
| Bursts cause 429s despite acceptable steady load | [Queuing](#23-queue-short-bursts-instead-of-immediately-rejecting-them) | Burst absorption, not extra compute | Workload group / Both |
| Queued ingestion creates many small extents | [Batching](#31-queued-ingestion-batching-throughput-versus-freshness) | Ingestion efficiency and lower merge pressure | Database/table / Both |
| ADX ingestion is slot-limited with spare resources | [Capacity](#33-adx-ingestion-operation-concurrency) | Concurrent data-management operations | Cluster / ADX only |
| Materialization falls behind | [Background capacity](#34-adx-materialization-export-and-maintenance-parallelism) and [views](#45-precompute-repeated-aggregations-with-materialized-views) | Background progress versus query load | Mixed; see sections |
| High disk misses / repeated dashboard work | [Cache and precomputation](#4-load-reduction-cache-data-layout-and-precomputation) | Less work per request | Database/table/request / Both |
| CPU is saturated while backlog grows | [Measurement and capacity](#5-measurement-capacity-and-a-safe-tuning-sequence) | Diagnose/scale or reduce demand | Product-specific |

**Parallelism** is workers within one operation. **Concurrency** is simultaneous
operations. **Throughput/load efficiency** is completed useful work per unit of
time/resource. Raising the first two does not guarantee the third.

## 0. Baseline: inspect before tuning

**Both.** These are independent commands; run them separately.

```kusto
.show databases

.show database MyDatabase details

.show tables details

.show table Events schema as json

.show running queries

.show queries
| where StartedOn > ago(1h)
| project StartedOn, Database, WorkloadGroup, Application, Duration,
          TotalCpu, MemoryPeak, State, FailureReason, CacheStatistics
| order by Duration desc

.show operations

.show ingestion failures
| where FailedOn > ago(1h)
| project FailedOn, Database, Table, FailureKind, ErrorCode, Details
```

`.show queries` is a recent, permission-scoped operational view, not a durable
audit history. A Database Admin/Monitor can see other users' database activity;
other users see their own queries. Persist diagnostic telemetry for long tests.
`.show ingestion failures` reports failures associated with ingestion management
commands, **not every failure in every queued-ingestion stage**. Also inspect
ingestion metrics, connector/SDK status, and diagnostic logs.

Sources: [query inspection][queries], [operations][operations],
[ingestion failures][ingestion-failures].

## 1. Parallelism: distribute work without overloading the service

### 1.1 Per-query fanout and resource ceilings

**Both; workload group.** Limiting fanout for a batch/ad-hoc workload can leave
resources for other requests. Raising fanout may accelerate one query but reduce
overall concurrency. First create the custom group using section 2.1.

```kusto
.show workload_group ['batch']

.alter-merge workload_group ['batch']
@'{
  "RequestLimitsPolicy": {
    "MaxFanoutThreadsPercentage": {"IsRelaxable": false, "Value": 50},
    "MaxFanoutNodesPercentage": {"IsRelaxable": false, "Value": 100},
    "MaxMemoryPerIterator": {"IsRelaxable": false, "Value": 1073741824},
    "MaxExecutionTime": {"IsRelaxable": false, "Value": "00:02:00"}
  }
}'

.show workload_group ['batch']
```

Choose memory limits within the node-dependent documented bounds. Small limits
can introduce query failures; large limits multiplied by many concurrent queries
can exhaust memory. Do not globally disable truncation/timeouts to mask failures.

For a request-local test, with no persistent policy change:

```kusto
set query_results_cache_max_age = time(0s);
set query_fanout_threads_percent = 50;
Events
| where Timestamp > ago(1h)
| summarize Count = count() by TenantId
```

Compare against the same query without the fanout setting. Request properties
cannot bypass a non-relaxable group limit.

Inspect the completed requests:

```kusto
.show queries
| where StartedOn > ago(15m)
| project StartedOn, WorkloadGroup, Duration, TotalCpu, MemoryPeak,
          State, FailureReason, ClientRequestProperties
```

Sources: [request limits][request-limits], [alter-merge workload group][alter-workload].

### 1.2 Shuffle high-cardinality aggregations and joins

**Both; request.** Start with the unhinted query, then compare shuffle for large,
high-cardinality grouping keys or operators hitting memory limits:

```kusto
Events
| where Timestamp > ago(1d)
| summarize hint.strategy=shuffle Count = count() by TenantId
```

For a high-cardinality join, filter/project both sides first:

```kusto
Events
| where Timestamp > ago(1h)
| project Timestamp, TenantId, Value
| join kind=inner hint.shufflekey=TenantId
    (TenantAttributes | project TenantId, Segment) on TenantId
```

Only test `hint.num_partitions` after measuring ordinary shuffle. More partitions
add overhead; skewed keys can still create hotspots. Verify the execution plan:
the optimizer can ignore shuffle in some operator combinations. Use the same
`.show queries` inspection as section 1.1 to compare CPU, memory, and duration.

Source: [shuffle query guidance][shuffle].

### 1.3 Union branch concurrency and node spread

**Both; request.** For independent union branches, compare the ordinary union with:

```kusto
union hint.concurrency=4 hint.spread=2
    (Events | where Timestamp > ago(1h) | project Timestamp, TenantId),
    (ArchivedEvents | where Timestamp > ago(1h) | project Timestamp, TenantId)
| summarize Count = count() by TenantId
```

`hint.concurrency` controls parallel union subqueries; `hint.spread` controls node
spread. These are not cluster-wide admission limits. Match them to available
nodes and meaningful branches; raising them can just create coordination work.
Compare `.show queries` statistics as in section 1.1.

Source: [union parameters][union].

## 2. Concurrency: admission control and workload isolation

### 2.1 Create a separate group, then route requests into it

**Both; Eventhouse/cluster.** Create the example `batch` group only if it does not
already exist. For an existing group, use `.alter-merge` after saving its policy;
`create-or-alter` replaces the definition.

```kusto
.show workload_groups

.create-or-alter workload_group ['batch']
@'{
  "RequestRateLimitPolicies": [
    {
      "IsEnabled": true,
      "Scope": "WorkloadGroup",
      "LimitKind": "ConcurrentRequests",
      "Properties": {"MaxConcurrentRequests": 8}
    }
  ]
}'

.show workload_group ['batch']
```

A group without classification normally receives no application traffic. The
following **replaces the global classifier**. Merge this branch into your
existing classifier instead of overwriting other routing rules:

```kusto
.show cluster policy request_classification

.alter cluster policy request_classification '{"IsEnabled":true}' <|
    case(
        request_properties.request_type == "Query"
            and request_properties.current_application == "BatchWorker",
        "batch",
        "default"
    )

.show cluster policy request_classification
```

Set the application's name in the **client request properties**, not a KQL
`set` statement used to try to influence classification. Keep classification
cheap. Application names are caller-controlled: use principal/group identity
conditions for governance that must not be bypassed by renaming a client.

```kusto
.show queries
| where StartedOn > ago(15m)
| summarize Requests = count(), Cpu = sum(TotalCpu)
    by WorkloadGroup, Application, State
```

Groups govern queries and management commands, **not streaming ingestion
requests**. They are resource governance, not dedicated CPU/memory reservations.

Sources: [workload groups][groups], [create group][create-workload],
[show groups][show-workload], [classification][classification],
[alter classifier][alter-classification].

### 2.2 Change concurrent requests or add per-principal protection

**Both; workload group.** This array demonstrates a group cap plus a principal
cap. Preserve existing rate-limit entries if they are still needed.

```kusto
.show workload_group ['batch']

.alter-merge workload_group ['batch']
@'{
  "RequestRateLimitPolicies": [
    {
      "IsEnabled": true,
      "Scope": "WorkloadGroup",
      "LimitKind": "ConcurrentRequests",
      "Properties": {"MaxConcurrentRequests": 8}
    },
    {
      "IsEnabled": true,
      "Scope": "Principal",
      "LimitKind": "ConcurrentRequests",
      "Properties": {"MaxConcurrentRequests": 4}
    }
  ]
}'

.show workload_group ['batch']
```

Both restrictions apply; a principal cannot consume all group slots. Change
limits in measured increments. A 429 whose origin identifies a workload-group
limit is different from ADX ingestion capacity exhaustion or Fabric capacity
throttling. Use bounded client concurrency and exponential backoff with jitter;
retrying immediately amplifies load.

Source: [request rate limits and throttling origins][rate-limits].

### 2.3 Queue short bursts instead of immediately rejecting them

**Both; workload group.** Requires a concurrent-request limit at
`Scope: WorkloadGroup`, such as section 2.2.

```kusto
.show workload_group ['batch']

.alter-merge workload_group ['batch']
@'{"RequestQueuingPolicy":{"IsEnabled":true}}'

.show workload_group ['batch']
```

Queuing waits for a bounded, service-defined short period. It is not a durable
job queue and does not increase processing capacity. Check end-to-end p95/p99
latency as well as the reduction in 429s. Sustained overload still needs less
demand, query optimization, or more compute.

Sources: [request queuing][queuing], [alter group examples][alter-workload].

### 2.4 Bound request rate or CPU consumption over a time window

**Both; workload group.** Use rate budgets to contain chatty clients or expensive
ad-hoc traffic. The array below preserves the example concurrent-request cap but
replaces any other previously configured entries:

```kusto
.show workload_group ['batch']

.alter-merge workload_group ['batch']
@'{
  "RequestRateLimitPolicies": [
    {
      "IsEnabled": true,
      "Scope": "WorkloadGroup",
      "LimitKind": "ConcurrentRequests",
      "Properties": {"MaxConcurrentRequests": 8}
    },
    {
      "IsEnabled": true,
      "Scope": "Principal",
      "LimitKind": "ResourceUtilization",
      "Properties": {
        "ResourceKind": "TotalCpuSeconds",
        "MaxUtilization": 1000,
        "TimeWindow": "01:00:00"
      }
    }
  ]
}'

.show workload_group ['batch']
```

For a request-count budget, the documented alternative is
`"ResourceKind":"RequestCount"` with an appropriate count and window. CPU budgets
use reports from completed requests, not a hard instantaneous CPU reservation.
Also inspect `RequestRateLimitsEnforcementPolicy` in the group output: enforcement
scope affects how limits are applied; do not infer a single global cap from a
query-head setting.

For an explicitly centralized limit, evaluate this **separate** change:

```kusto
.show workload_group ['batch']

.alter-merge workload_group ['batch']
@'{
  "RequestRateLimitsEnforcementPolicy": {
    "QueriesEnforcementLevel":"Cluster",
    "CommandsEnforcementLevel":"Cluster"
  }
}'

.show workload_group ['batch']
```

Default `QueryHead`/`Database` enforcement can effectively multiply a configured
limit across the participating query heads/database admins. `Cluster` centralizes
enforcement, which can itself add coordination pressure. Changing scope can
therefore substantially change effective admission even without changing the
numeric limits; load-test it independently.

Sources: [rate budgets][rate-limits], [enforcement scope][enforcement].

### 2.5 Reduce query-head pressure with an approved consistency trade-off

**Both; workload group.** Weak consistency can spread query-head work but may
temporarily omit newly ingested data or recent schema changes. It is not a
general acceleration switch for CPU-bound operators.

```kusto
.show workload_group ['batch']

.alter-merge workload_group ['batch']
@'{
  "QueryConsistencyPolicy": {
    "QueryConsistency": {"IsRelaxable": false, "Value": "Weak"}
  }
}'

.show workload_group ['batch']
```

Keep read-after-write workloads strong unless the business accepts the trade-off.

**ADX only; advanced service configuration.** If the bottleneck is specifically
the number of weak-consistency query heads, inspect the cluster service policy.
The following is an illustrative, support-reviewed adjustment, not a Fabric
command or a general recommendation:

```kusto
.show cluster policy query_weak_consistency

.alter-merge cluster policy query_weak_consistency
@'{"PercentageOfNodes":30}'

.show cluster policy query_weak_consistency
```

Existing minimum/maximum node settings still apply. Do not casually alter metadata
prefetch, refresh, lag, or super-slacker settings; the documentation calls for
support consultation for sensitive service-policy properties.

Sources: [group consistency policy][consistency],
[ADX weak-consistency service policy][weak-policy],
[alter weak-consistency policy][alter-weak].

## 3. Ingestion and background load: throughput versus latency

### 3.1 Queued ingestion batching: throughput versus freshness

**Both; database/table.** Batch sealing occurs when the first size, item-count,
or time threshold is reached. The documented defaults are 5 minutes, 500 items,
and 1024 MB of **uncompressed** data.

Inspect database and table overrides before changing either:

```kusto
.show database MyDatabase policy ingestionbatching

.show table Events policy ingestionbatching
```

For a throughput-oriented database default, this example uses the documented
default thresholds:

```kusto
.alter database MyDatabase policy ingestionbatching
@'{
  "MaximumBatchingTimeSpan":"00:05:00",
  "MaximumNumberOfItems":500,
  "MaximumRawDataSizeMB":1024
}'

.show database MyDatabase policy ingestionbatching
```

For an individual table with a tighter freshness requirement, change only time:

```kusto
.alter-merge table Events policy ingestionbatching
@'{"MaximumBatchingTimeSpan":"00:00:30"}'

.show table Events policy ingestionbatching
```

Database policy changes apply to tables without explicit overrides. A table
override takes precedence. Very short batches on low-volume tables create tiny
extents, extra ingestion/merge work, and can even increase actual latency.
Reducing the batching timer does not fix a compute-bound ingestion backlog.
Avoid per-record queued submissions, unnecessary `FlushImmediately`, and excessive
unique ingestion properties/tags that fragment batching.

Sources: [batching policy, limits, and trade-offs][batching],
[alter database batching][alter-db-batching],
[alter-merge table batching][merge-table-batching],
[ingestion best practices][ingest-best-practices].

### 3.2 Streaming ingestion: latency, not maximum bulk throughput

**ADX only for the policy command below.** The current public command page is
ADX-scoped. Fabric supports streaming ingestion, but this is not proof that the
same administration command is supported there; use its documented ingestion
setup and product capabilities.

```kusto
.show table Events policy streamingingestion

.alter table Events policy streamingingestion '{"IsEnabled":true}'

.show table Events policy streamingingestion
```

ADX must also have streaming ingestion enabled at the cluster level through its
control plane. A table policy alone does not enable that prerequisite or switch
an existing producer from queued to streaming ingestion. Choose queued ingestion
for sustained high-volume bulk workloads; test streaming for low-latency, small
payload workloads. Workload-group request limits do not govern streaming requests.

Sources: [streaming policy command][streaming-command],
[ADX streaming setup][streaming-setup], [Fabric ingestion][fabric-ingest].

### 3.3 ADX ingestion operation concurrency

**ADX only; cluster.** Inspect the policy and the **effective** operation slots:

```kusto
.show cluster policy capacity

.show capacity ingestions

.alter-merge cluster policy capacity
@'{
  "IngestionCapacity": {
    "ClusterMaximumConcurrentOperations":128,
    "CoreUtilizationCoefficient":0.5
  }
}'

.show cluster policy capacity

.show capacity ingestions
```

Effective ingestion capacity is the minimum of the cluster cap and a
node/core/coefficient-derived limit. On clusters with at least four nodes, the
admin node is excluded from participating ingestion nodes. Increasing the cap
does nothing if the core-derived limit is lower. It also does not enlarge the
cluster or bypass workload-group throttling.

Raise slots only for demonstrated slot saturation with CPU/memory/I/O headroom.
More simultaneous ingestion can starve querying, materialization, and maintenance.
Allow for propagation and measure effective capacity rather than assuming an
immediate change.

Use **`.alter-merge`**, not `.alter cluster policy capacity`: the latter is
documented as deprecated and resets omitted settings to defaults.

Sources: [capacity calculations][capacity], [show capacity][show-capacity],
[alter-merge capacity][merge-capacity], [deprecated alter command][alter-capacity].

### 3.4 ADX materialization, export, and maintenance parallelism

**ADX only; cluster.** These components share the same capacity-policy command,
but address different background operations. Change only the component matching
the measured backlog.

Materialization concurrency:

```kusto
.show cluster policy capacity

.show capacity materialized-view

.alter-merge cluster policy capacity
@'{
  "MaterializedViewsCapacity": {
    "ClusterMinimumConcurrentOperations":2,
    "ClusterMaximumConcurrentOperations":10
  }
}'

.show cluster policy capacity

.show capacity materialized-view
```

The service normally adapts concurrency. Raising the minimum forces more
materializations to compete at once and can exhaust resources. This does not
repair an inefficient materialized-view definition.

Export concurrency:

```kusto
.show cluster policy capacity

.show capacity data-export

.alter-merge cluster policy capacity
@'{"ExportCapacity":{"ClusterMaximumConcurrentOperations":16}}'

.show cluster policy capacity

.show capacity data-export
```

The existing export coefficient can remain the binding limit. Check destination
storage throttling as well as query/ingestion impact.

For partitioning or small-extent maintenance backlogs, inspect first:

```kusto
.show cluster policy capacity

.show capacity extents-partition

.show capacity extents-merge
```

Other advanced capacity components include `ExtentsPartitionCapacity`,
`ExtentsMergeCapacity`, and `StreamingIngestionPostProcessingCapacity`.
Do not raise all of them together. Merge-capacity changes deserve support review;
often fixing tiny ingestion batches is better than adding merge concurrency.

Sources: [capacity components and warnings][capacity], [show capacity][show-capacity].

### 3.5 Enable ingestion timestamps for freshness diagnostics

**Both; table.** This is an observability policy, **not** a throughput or
concurrency accelerator:

```kusto
.show table Events policy ingestiontime

.alter table Events policy ingestiontime true

.show table Events policy ingestiontime
```

Enabling it records approximate ingestion timestamps for subsequently ingested
records, available through `ingestion_time()`. It does not retroactively stamp old
rows or guarantee event ordering. Compare event timestamps, producer timestamps,
and ingestion timestamps to diagnose freshness; their differences can include
upstream delay and clock skew, not just service processing time.

Sources: [alter ingestion-time policy][ingestiontime],
[show ingestion-time policy][show-ingestiontime].

## 4. Load reduction: cache, data layout, and precomputation

### 4.1 Right-size hot cache separately from retention

**Both; database/table.** Keep frequently queried time ranges hot without
indiscriminately expanding every table's cache footprint.

```kusto
.show database MyDatabase policy caching

.alter database MyDatabase policy caching hot = 7d

.show database MyDatabase policy caching

.show table Events policy caching

.alter table Events policy caching hot = 3d

.show table Events policy caching
```

The table override wins over the database default. Match the hot window to actual
query ranges and available premium/SSD cache. Expanding it does not add memory or
SSD, and warming data takes time. Reading cold data remains possible.
`DataScope: HotCache` is **not** an equivalent optimization: it changes which data
a query can return and may omit required history.

Sources: [cache policy][cache], [alter table caching][alter-cache],
[alter database caching][alter-db-cache].

### 4.2 Retain only the history the workload actually needs

**Both; database/table.** Obtain data-owner approval before shortening retention.

```kusto
.show database MyDatabase policy retention

.alter-merge database MyDatabase policy retention
    softdelete = 90d recoverability = enabled

.show database MyDatabase policy retention

.show table Events policy retention

.alter-merge table Events policy retention
    softdelete = 30d recoverability = enabled

.show table Events policy retention
```

Retention primarily controls data lifecycle/storage, not worker slots or query
admission. It is based on extent age, not automatically on an event timestamp
column. Table overrides take precedence. Deletion is asynchronous; do not treat
retention as an immediate purge or guaranteed exact-time deletion.
Restoring the old policy does not bring back already removed data.

Sources: [retention policy][retention], [alter-merge table retention][merge-retention],
[alter-merge database retention][merge-db-retention].

### 4.3 Partition only when workload shape justifies the rewrite cost

**Both; table.** Most tables do not need an explicit partitioning policy.
For frequent equality filters on a sufficiently large, medium/high-cardinality
tenant key:

```kusto
.show table Events policy partitioning

.alter table Events policy partitioning
@'{
  "PartitionKeys": [
    {
      "ColumnName":"TenantId",
      "Kind":"Hash",
      "Properties": {
        "Function":"XxHash64",
        "MaxPartitionCount":128,
        "PartitionAssignmentMode":"Uniform"
      }
    }
  ]
}'

.show table Events policy partitioning
```

For large, evenly distributed high-cardinality joins/aggregations, evaluate
`ByPartition` instead of `Uniform` using the documented scenario guidance.
These are alternatives, not successive settings to apply universally.

Partitioning reingests extents in the background, increasing CPU, storage
transactions, and residual artifacts. The docs discourage partitions with less
than roughly 1 GB compressed data each. Increasing partition count can hurt
instead of help. For event-time partitioning, understand
`OverrideCreationTime` before using it: it can change retention/cache age behavior.

Sources: [supported partitioning scenarios and cautions][partitioning],
[alter partitioning][alter-partitioning].

### 4.4 Row order and extent merging

**Both; table.** Row order may help selective workloads over a small set of
sortable values:

```kusto
.show table Events policy roworder

.alter table Events policy roworder (TenantId asc, Timestamp desc)

.show table Events policy roworder
```

Use at most two sort keys as recommended. This is physical ordering inside
extents, not a guarantee of query result order. Do not redundantly add a hash
partition key to row order. Test ingestion cost as well as read latency.

For fragmentation, inspect extents and merge settings before any change:

```kusto
.show table Events details

.show table Events extents
| summarize Extents = count(),
            Rows = sum(RowCount),
            OriginalBytes = sum(OriginalSize),
            ExtentBytes = sum(ExtentSize)

.show table Events policy merge

.show table Events policy retention

.show table Events policy caching
```

For extent counts/sizes alone, prefer `.show table Events details`. Detailed
extent enumeration can itself be expensive on very large tables; use it only
when shard-level information is needed, at the narrowest appropriate scope.

**Support-reviewed example only:** if the effective retention/hot-cache minimum
is 30 days, Microsoft's merge guidance gives an 18-hour merge range:

```kusto
.alter-merge table Events policy merge '{"MaxRangeInHours":18}'

.show table Events policy merge
```

Do not copy this range for a 3-day hot cache. The merge guidance bases the range
on roughly 2-3% of `min(retention, hot-cache duration)`. Do not disable
merge/rebuild or change `MaxExtentsToMerge` as a generic tuning step.
Different `drop-by` tags can prevent merging even with a healthy policy.

Sources: [row order command][roworder], [merge policy and support warning][merge],
[alter-merge merge policy][alter-merge-policy], [extent inspection][extents].

### 4.5 Precompute repeated aggregations with materialized views

**Both; database/view.** Move repeated aggregation cost from every reader to
incremental background materialization. Check for an existing view first:

```kusto
.show materialized-views

.create materialized-view EventsDaily on table Events
{
    Events
    | summarize EventCount = count() by TenantId, bin(Timestamp, 1d)
}

.show materialized-view EventsDaily
```

This creates a view **from now onward**, not a historical backfill. If historical
coverage is required, plan the documented asynchronous backfill separately and
monitor `.show operations`. Avoid launching large backfills during peak load.

Tune the view's own cache independently:

```kusto
.show materialized-view EventsDaily policy caching

.alter materialized-view EventsDaily policy caching hot = 7d

.show materialized-view EventsDaily policy caching
```

Querying `EventsDaily` combines its materialized portion with the unmaterialized
source delta. A cold source can still cause cache misses. The view and source have
independent retention; do not set zero retention on an MV source. Check view
health/materialization age with `.show materialized-view EventsDaily`, and compare
end-to-end query cost before/after. Views shift work; they do not make ingestion
or storage free.

Sources: [create materialized view][create-mv], [show materialized view][show-mv],
[materialized-view policy behavior][mv-policies].

### 4.6 Parse/project once at ingestion using an update policy

**Both; table.** An update policy can replace repeated query-time transformations,
but adds ingestion work and can amplify writes across many target tables.
Inspect target schema and every existing policy entry first:

```kusto
.show table CuratedEvents schema as json

.show table CuratedEvents policy update

.show table Events schema as json
```

For an existing target with exactly `(Timestamp:datetime, TenantId:string,
Value:real)`, this example projects matching source columns. Substitute a tested,
schema-compatible transformation for a real use case.

```kusto
.alter table CuratedEvents policy update
@'[
  {
    "IsEnabled":true,
    "Source":"Events",
    "Query":"Events | project Timestamp, TenantId, Value",
    "IsTransactional":true,
    "PropagateIngestionProperties":false
  }
]'

.show table CuratedEvents policy update
```

This **replaces the entire target update-policy array**. Preserve other required
entries; do not blindly paste it into an existing pipeline. Source and target must
be in the same database. Column names, types, and order must match the target.
Transactional failures can fail source ingestion too; test this deliberately.
Do not disable transactionality merely to hide ingestion errors or silently lose
derived data.

Sources: [alter update policy][update-command], [update-policy behavior][update].

### 4.7 Reuse results for repeated dashboard queries

**Both; request/database.** This avoids recomputation when bounded staleness is
acceptable; it is separate from the hot-data cache.

```kusto
.show database cache query_results
```

Run the following exact query twice:

```kusto
set query_results_cache_max_age = time(1m);
Events
| where Timestamp > ago(1h)
| summarize EventCount = count() by TenantId
```

Then inspect again:

```kusto
.show database cache query_results
```

Inspect response extended properties for `ServerCache` to verify an individual
hit; cache counters alone also include other callers. Identity of query text,
database, and relevant request properties matters. Query results over 16 MB and
certain queries (including RLS-protected tables and external data) are not eligible.
For benchmarks, use `query_results_cache_max_age = time(0s)` unless measuring
result-cache behavior specifically.

A group-level default can be set independently:

```kusto
.show workload_group ['batch']

.alter-merge workload_group ['batch']
@'{
  "QueryConsistencyPolicy": {
    "CachedResultsMaxAge": {"IsRelaxable":true,"Value":"00:01:00"}
  }
}'

.show workload_group ['batch']
```

Sources: [query results cache][results-cache], [cache inspection][show-results-cache],
[group consistency/cache policy][consistency].

## 5. Measurement, capacity, and a safe tuning sequence

### Compare behavior, not just configured values

Use a fixed dataset/time window and identical query mix for each comparison.
Avoid `ago()` window drift for controlled benchmarks by substituting fixed
datetime boundaries. Label load-test requests through the client application's
request properties and collect client latency, including retries/queue time.

```kusto
.show queries
| where StartedOn > ago(1h) and Application == "LoadTest"
| extend DurationMs = Duration / 1ms, CpuSeconds = TotalCpu / 1s
| summarize Requests = count(),
            Failed = countif(State != "Completed"),
            P50Ms = percentile(DurationMs, 50),
            P95Ms = percentile(DurationMs, 95),
            P99Ms = percentile(DurationMs, 99),
            CpuSeconds = sum(CpuSeconds),
            PeakMemoryBytes = max(MemoryPeak)
    by WorkloadGroup, bin(StartedOn, 5m)
| order by StartedOn asc
```

This query summarizes visible recent **server-side** query telemetry. It is not
a substitute for client-side success rate and end-to-end latency: requests
rejected upstream, retries, or queueing may not be fully represented.

Track all of the following before and after each individual change:

| Area | Measure / inspect |
| --- | --- |
| Query service | Completed queries/sec, client p95/p99, 429 origin, failures, CPU/query, memory/query, data-cache misses |
| Ingestion | Records/bytes/sec, queue depth/oldest age, batching latency versus processing latency, errors, freshness |
| Background | MV age/health, backfill progress, partition/merge backlog, export failures |
| Resources | CPU, memory, cache utilization, storage/network bottlenecks, actual cost/CUs |
| Correctness | Same expected rows/aggregates; explicitly accepted staleness, retention, and historical coverage |

### Capacity is not just a KQL policy

- **Fabric:** inspect Eventhouse System overview, ingestion/query monitoring,
  Capacity Planner mode (previously Always-On), minimum capacity schedules,
  and Fabric Capacity Metrics/throttling. Capacity scaling and these Eventhouse
  controls are portal/control-plane operations, not
  `.alter cluster policy capacity`. Increasing minimum consumption can reduce
  cold-start/scaling delays but increases cost and does not eliminate capacity
  contention.
- **ADX:** inspect cluster SKU, instance count, optimized autoscale, CPU/cache
  utilization, ingestion queue metrics, and storage/network limits. Scale up/out
  through the Azure control plane when the bottleneck is actual compute/cache,
  not an artificial admission cap.
- **Both:** filter datetime early, project unused columns away, avoid repeated
  parsing, choose join strategy based on input sizes, and reduce dashboard refresh
  frequency before raising global limits.

### Suggested order

1. Establish correctness, throughput, freshness, latency, and cost targets.
2. Capture policies and baseline representative concurrent query **and ingestion**
   traffic. Separate cold-start, warm-cache, and result-cache measurements.
3. Reduce work: query shape, batching, cache sizing, repeated parsing/aggregation.
4. Isolate workloads and set bounded admission, queuing, and resource limits.
5. Test request-local parallelism; then evaluate layout/background changes only
   where their workload-specific benefits justify the ingestion/rewrite cost.
6. For ADX, adjust operation capacities only when effective slots are the measured
   bottleneck and resources are available. For Fabric, use its supported capacity
   controls instead.
7. Change one variable, wait for the relevant stabilization/background processing,
   replay the same load, and compare the entire metric set. Keep only changes
   that meet targets without correctness or ingestion regressions.
8. Restore saved settings if the agreed targets regress. Confirm policy readback
   and behavior after rollback.

Sources: [query best practices][query-best-practices],
[Fabric Eventhouse monitoring][fabric-monitor],
[Fabric capacity controls][fabric-capacity],
[ADX optimized autoscale][autoscale].

## Engineering blogs reviewed

Blogs provide operational context; the current Learn command page is the authority
for syntax, support, and limits. Older ADX/Synapse examples are not evidence that
an administrative setting is supported in Fabric.

| Blog | Key lesson used here |
| --- | --- |
| [Performance Optimizations for Applications using Azure Data Explorer][blog-performance] (2022) | Separate hot-data caching, results caching, ingestion-time transformations, retention, and precomputation instead of solving every problem with more concurrency. |
| [Evaluate query performance of Azure Data Explorer][blog-loadtest] (2022) | Load-test expected query/ingestion traffic with realistic concurrency, suitable sizing, and consistent measurements; do not infer service capacity from a single fast query. |
| [Monitor batching ingestion with ADX Insights][blog-batching] (2021, updated 2022) | Distinguish batch formation from pipeline processing/queue latency before changing batching thresholds. |

## Microsoft Learn command and policy sources

Links are attached to the relevant sections above; the following entry points
are useful when extending this reference:

- [Policy overview][policies]
- [Workload groups][groups]
- [Capacity policy (ADX)][capacity]
- [Ingestion batching policy][batching]
- [Fabric Eventhouse management and monitoring][fabric-monitor]

[policies]: https://learn.microsoft.com/en-us/kusto/management/policies?view=microsoft-fabric
[queries]: https://learn.microsoft.com/en-us/kusto/management/show-queries-command?view=microsoft-fabric
[operations]: https://learn.microsoft.com/en-us/kusto/management/show-operations?view=microsoft-fabric
[ingestion-failures]: https://learn.microsoft.com/en-us/kusto/management/ingestion-failures?view=microsoft-fabric
[groups]: https://learn.microsoft.com/en-us/kusto/management/workload-groups?view=microsoft-fabric
[create-workload]: https://learn.microsoft.com/en-us/kusto/management/create-or-alter-workload-group-command?view=microsoft-fabric
[show-workload]: https://learn.microsoft.com/en-us/kusto/management/show-workload-group-command?view=microsoft-fabric
[alter-workload]: https://learn.microsoft.com/en-us/kusto/management/alter-merge-workload-group-command?view=microsoft-fabric
[request-limits]: https://learn.microsoft.com/en-us/kusto/management/request-limits-policy?view=microsoft-fabric
[classification]: https://learn.microsoft.com/en-us/kusto/management/request-classification-policy?view=microsoft-fabric
[alter-classification]: https://learn.microsoft.com/en-us/kusto/management/alter-cluster-policy-request-classification-command?view=microsoft-fabric
[rate-limits]: https://learn.microsoft.com/en-us/kusto/management/request-rate-limit-policy?view=microsoft-fabric
[enforcement]: https://learn.microsoft.com/en-us/kusto/management/request-rate-limits-enforcement-policy?view=microsoft-fabric
[queuing]: https://learn.microsoft.com/en-us/kusto/management/request-queuing-policy?view=microsoft-fabric
[consistency]: https://learn.microsoft.com/en-us/kusto/management/query-consistency-policy?view=microsoft-fabric
[weak-policy]: https://learn.microsoft.com/en-us/kusto/management/query-weak-consistency-policy?view=azure-data-explorer
[alter-weak]: https://learn.microsoft.com/en-us/kusto/management/alter-query-weak-consistency-policy?view=azure-data-explorer
[shuffle]: https://learn.microsoft.com/en-us/kusto/query/shuffle-query?view=microsoft-fabric
[union]: https://learn.microsoft.com/en-us/kusto/query/union-operator?view=microsoft-fabric
[batching]: https://learn.microsoft.com/en-us/kusto/management/batching-policy?view=microsoft-fabric
[alter-db-batching]: https://learn.microsoft.com/en-us/kusto/management/alter-database-ingestion-batching-policy?view=microsoft-fabric
[merge-table-batching]: https://learn.microsoft.com/en-us/kusto/management/alter-merge-table-ingestion-batching-policy?view=microsoft-fabric
[ingest-best-practices]: https://learn.microsoft.com/en-us/kusto/api/netfx/kusto-ingest-best-practices
[streaming-command]: https://learn.microsoft.com/en-us/kusto/management/alter-table-streaming-ingestion-policy-command?view=azure-data-explorer
[streaming-setup]: https://learn.microsoft.com/en-us/azure/data-explorer/ingest-data-streaming
[fabric-ingest]: https://learn.microsoft.com/en-us/fabric/real-time-intelligence/get-data-overview
[capacity]: https://learn.microsoft.com/en-us/kusto/management/capacity-policy?view=azure-data-explorer
[show-capacity]: https://learn.microsoft.com/en-us/kusto/management/show-capacity-command?view=azure-data-explorer
[merge-capacity]: https://learn.microsoft.com/en-us/kusto/management/alter-merge-capacity-policy-command?view=azure-data-explorer
[alter-capacity]: https://learn.microsoft.com/en-us/kusto/management/alter-capacity-policy-command?view=azure-data-explorer
[ingestiontime]: https://learn.microsoft.com/en-us/kusto/management/alter-ingestion-time-policy-command?view=microsoft-fabric
[show-ingestiontime]: https://learn.microsoft.com/en-us/kusto/management/show-table-ingestion-time-policy-command?view=microsoft-fabric
[cache]: https://learn.microsoft.com/en-us/kusto/management/cache-policy?view=microsoft-fabric
[alter-cache]: https://learn.microsoft.com/en-us/kusto/management/alter-table-cache-policy-command?view=microsoft-fabric
[alter-db-cache]: https://learn.microsoft.com/en-us/kusto/management/alter-database-cache-policy-command?view=microsoft-fabric
[retention]: https://learn.microsoft.com/en-us/kusto/management/retention-policy?view=microsoft-fabric
[merge-retention]: https://learn.microsoft.com/en-us/kusto/management/alter-merge-table-retention-policy-command?view=microsoft-fabric
[merge-db-retention]: https://learn.microsoft.com/en-us/kusto/management/alter-merge-database-retention-policy-command?view=microsoft-fabric
[partitioning]: https://learn.microsoft.com/en-us/kusto/management/partitioning-policy?view=microsoft-fabric
[alter-partitioning]: https://learn.microsoft.com/en-us/kusto/management/alter-table-partitioning-policy-command?view=microsoft-fabric
[roworder]: https://learn.microsoft.com/en-us/kusto/management/alter-table-row-order-policy-command?view=microsoft-fabric
[merge]: https://learn.microsoft.com/en-us/kusto/management/merge-policy?view=microsoft-fabric
[alter-merge-policy]: https://learn.microsoft.com/en-us/kusto/management/alter-merge-table-merge-policy-command?view=microsoft-fabric
[extents]: https://learn.microsoft.com/en-us/kusto/management/show-extents?view=microsoft-fabric
[create-mv]: https://learn.microsoft.com/en-us/kusto/management/materialized-views/materialized-view-create?view=microsoft-fabric
[show-mv]: https://learn.microsoft.com/en-us/kusto/management/materialized-views/materialized-view-show-command?view=microsoft-fabric
[mv-policies]: https://learn.microsoft.com/en-us/kusto/management/materialized-views/materialized-view-policies?view=microsoft-fabric
[update-command]: https://learn.microsoft.com/en-us/kusto/management/alter-table-update-policy-command?view=microsoft-fabric
[update]: https://learn.microsoft.com/en-us/kusto/management/update-policy?view=microsoft-fabric
[results-cache]: https://learn.microsoft.com/en-us/kusto/query/query-results-cache?view=microsoft-fabric
[show-results-cache]: https://learn.microsoft.com/en-us/kusto/management/show-query-results-cache-command?view=microsoft-fabric
[query-best-practices]: https://learn.microsoft.com/en-us/kusto/query/best-practices?view=microsoft-fabric
[fabric-monitor]: https://learn.microsoft.com/en-us/fabric/real-time-intelligence/manage-monitor-eventhouse
[fabric-capacity]: https://learn.microsoft.com/en-us/fabric/real-time-intelligence/eventhouse-smart-capacity-control
[autoscale]: https://learn.microsoft.com/en-us/azure/data-explorer/manage-cluster-horizontal-scaling
[blog-performance]: https://techcommunity.microsoft.com/blog/azuredataexplorer/performance-optimizations-for-applications-using-azure-data-explorer/3262996
[blog-loadtest]: https://techcommunity.microsoft.com/blog/azuredataexplorer/evaluate-query-performance-of-azure-data-explorer/3618005
[blog-batching]: https://techcommunity.microsoft.com/blog/azuredataexplorer/monitor-batching-ingestion-with-adx-insights/2673509
