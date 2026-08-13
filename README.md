# FabricOps

FabricOps is a personal test repository for Microsoft Fabric code, deployment
patterns, and proof-of-concept workloads.

The projects are intended for learning and experimentation. Review resource
names, capacities, credentials, retention settings, and cost implications before
using them outside a development environment.

## Projects

| Project | Description |
| --- | --- |
| [TickPOC](TickPOC/README.md) | C++ Apache Avro stock-ticker generator with selectable Eventhouse streaming and queued ingestion. |
| [NYC Taxi Medallion](NYC_Taxi_Medallion/README.md) | Bronze, Silver, and Gold lakehouse architecture using NYC TLC taxi data, Fabric notebooks, pipelines, Direct Lake, and Power BI. |

## Authentication

The examples use Microsoft Entra authentication. For interactive development,
sign in through Azure CLI:

```powershell
az login --use-device-code
```

Do not commit bearer tokens, client secrets, local token caches, or generated
build output.

Copy [.env.example](.env.example) to `.env` and provide local endpoints and
identifiers there. `.env` is excluded from Git; the committed example contains
placeholders only. Keep access tokens transient in the process environment
rather than writing them to `.env`.

## Repository approach

Each project keeps its own prerequisites and execution instructions in its
folder. Fabric workspace and item IDs are environment-specific and should be
discovered dynamically rather than copied between tenants.
