# Kusto command guides

Two complementary workflows for Fabric Eventhouse and KQL databases:

| Guide | Purpose | KQL companion |
| --- | --- | --- |
| [Optimization guide](Optimizations.README.md) | Tune parallelism, concurrency, ingestion, cache, retention, and data layout. Includes public documentation, ADX/Eventhouse support labels, and inspection/change/readback pairs. **Contains state-changing commands.** | [Optimizations.kql](Optimizations.kql) |
| [Workspace monitoring guide](WorkspaceMonitoring.README.md) | Investigate ingestion backlog, slow or rejected queries, capacity throttling, scale-out reasons, and reduced sizing using Fabric workspace monitoring. **Read-only troubleshooting queries.** | [WorkspaceMonitoring.kql](WorkspaceMonitoring.kql) |

Start with **workspace monitoring** to identify the bottleneck, then use the
**optimization guide** to evaluate a targeted change.

Run individual selections from either KQL file, not the whole file. Follow each
guide's database-context instructions, replace sample identifiers, and review
permissions and prerequisites. No example has been executed against your live
environment.
