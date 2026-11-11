# FabricOps

FabricOps is a personal repository for Microsoft Fabric code, deployment
patterns, and proof-of-concept workloads.

The projects are intended for documenting new concepts and documentations. Review resource
names, capacities, credentials, retention settings, and cost implications before
using them outside a development environment.

## Projects

| Project | Description |
| --- | --- |
| [EventhouseKustoIngest](EventhouseKustoIngest/README.md) | C++ Apache Avro stock-ticker generator with selectable Eventhouse streaming and queued ingestion. |
| [EventHubTickerProducer](EventHubTickerProducer/README.md) | C++ Apache Avro stock-ticker producer using passwordless Azure Event Hubs publishing and direct Fabric Eventhouse ingestion. |
| [eventhubAvro](eventhubAvro/README.md) | Customer-ready, single-file C++ sample for publishing Avro Object Container events to Azure Event Hubs. |
| [KustoCommands](KustoCommands/README.md) | Source-linked Eventhouse/ADX optimization commands and read-only Fabric workspace monitoring queries for ingestion, performance, throttling, and scaling investigations. |
| [NYC Taxi Medallion](NYC_Taxi_Medallion/README.md) | Bronze, Silver, and Gold lakehouse architecture using NYC TLC taxi data, Fabric notebooks, pipelines, Direct Lake, and Power BI. |
| [RayfinApps](RayfinApps/) | Microsoft Fabric App templates: [Finance Analytics](RayfinApps/finance-analytics/README.md) with illustrative offline data, and [Helsinki Public Transport](RayfinApps/helsinki-public-transport/README.md) with live transit maps backed by Fabric Real-Time Intelligence. |

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

To load the root `.env` into a PowerShell session without executing its contents:

```powershell
$envFile = Join-Path (Get-Location) ".env"
$entries = @{}
$lineNumber = 0
foreach ($line in Get-Content -LiteralPath $envFile -Encoding utf8 -ErrorAction Stop) {
  $lineNumber++
  $entry = $line.Trim()
  if (-not $entry -or $entry.StartsWith('#')) { continue }
  if ($entry -notmatch '^([A-Za-z_][A-Za-z0-9_]*)\s*=(.*)$') {
    throw "Invalid environment entry at line $lineNumber."
  }
  $name = $Matches[1]
  $value = $Matches[2].Trim()
  if ($value.Length -ge 2 -and (
    ($value.StartsWith('"') -and $value.EndsWith('"')) -or
    ($value.StartsWith("'") -and $value.EndsWith("'"))
  )) {
    $value = $value.Substring(1, $value.Length - 2)
  }
  $entries[$name] = $value
}
foreach ($name in $entries.Keys) {
  if ($null -eq [Environment]::GetEnvironmentVariable($name, 'Process')) {
    [Environment]::SetEnvironmentVariable($name, $entries[$name], 'Process')
  }
}
```

Then validate the Azure CLI settings and authenticate:

```powershell
foreach ($name in 'AZURE_TENANT_ID', 'AZURE_SUBSCRIPTION_ID') {
  $value = [Environment]::GetEnvironmentVariable($name, 'Process')
  if ([string]::IsNullOrWhiteSpace($value) -or $value.Contains('<')) {
    throw "$name is required; replace its placeholder in the environment file."
  }
}
az login --use-device-code --tenant $env:AZURE_TENANT_ID
az account set --subscription $env:AZURE_SUBSCRIPTION_ID
```

The loading block preserves variables already set in the process and rejects
malformed entries without printing their contents. It supports
plain `NAME=value` entries, full-line comments, and single- or double-quoted
literal values; shell expansion and inline comments are not supported.

Keep subscription, tenant, workspace, connection, and application IDs, personal
account names, and deployed hostnames in ignored environment files, not source or
documentation. Rayfin's YAML uses environment interpolation; import the root file
before invoking the CLI from an app folder. Generated Rayfin runtime properties
remain in each app's ignored `rayfin/.env`, not in the shared root file.
Browser-visible `VITE_*` values are public configuration, never a place for secrets.

Sanitizing working files does not remove identifiers from existing Git history.
Before republishing, create a clean history from the sanitized file snapshot;
do not push the old branches or tags.

## Repository approach

Each project keeps its own prerequisites and execution instructions in its
folder. Fabric workspace and item IDs are environment-specific and should be
discovered dynamically rather than copied between tenants.
