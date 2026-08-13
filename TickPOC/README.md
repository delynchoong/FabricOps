# TickPOC

TickPOC generates simulated stock-ticker events and ingests them into a
Microsoft Fabric Eventhouse configured through local environment variables.

The generator uses five ticker symbols:

- AAPL
- MSFT
- NVDA
- AMZN
- GOOGL

Prices follow a small random walk and are constrained to within 10% of each
ticker's starting price. The default rate is 100 events per second.

## Event schema

| Field | Type | Example |
| --- | --- | --- |
| `eventname` | string | `stock ticks` |
| `eventtime` | datetime | Current UTC time with millisecond precision |
| `ticker` | string | `MSFT` |
| `price` | real | `425.75` |
| `eventdesc` | string | `stock ticker price` |

Events are serialized as Apache Avro and use the ingestion mapping configured
in `KUSTO_MAPPING`.

## Prerequisites

- Azure CLI
- CMake
- Visual Studio 2022 Build Tools with the C++ workload
- vcpkg
- Access to a Fabric Eventhouse with the target table and Avro mapping

Sign in with a device code:

```powershell
az login --use-device-code
```

## Build

Open PowerShell from the repository root and set the project paths:

```powershell
$repositoryRoot = (Get-Location).Path
$project = Join-Path $repositoryRoot "TickPOC\cpp"
$vcpkgRoot = Join-Path $env:USERPROFILE "vcpkg"
$cmake = "C:\Program Files (x86)\Microsoft Visual Studio\2022\BuildTools\Common7\IDE\CommonExtensions\Microsoft\CMake\CMake\bin\cmake.exe"
```

Configure the x64 build:

```powershell
& $cmake `
  -S $project `
  -B "$project\build-x64" `
  -G "Visual Studio 17 2022" `
  -A x64 `
  -DCMAKE_TOOLCHAIN_FILE="$vcpkgRoot\scripts\buildsystems\vcpkg.cmake" `
  -DVCPKG_TARGET_TRIPLET=x64-windows `
  -DVCPKG_HOST_TRIPLET=x64-windows
```

Compile:

```powershell
& $cmake --build "$project\build-x64" --config Release
```

The generator is created at:

```text
TickPOC\cpp\build-x64\Release\stock_ticker_generator.exe
```

After changing the source, only the compile command normally needs to be run
again.

## Authentication

Copy the safe template and update the local values:

```powershell
Copy-Item (Join-Path $repositoryRoot ".env.example") `
  (Join-Path $repositoryRoot ".env")
```

The local `.env` is ignored by Git. It contains the Eventhouse endpoints and
target object names:

```text
KUSTO_QUERY_URI=https://<eventhouse-host>.kusto.fabric.microsoft.com
KUSTO_INGEST_URI=https://ingest-<eventhouse-host>.kusto.fabric.microsoft.com
KUSTO_DATABASE=<database-name>
KUSTO_TABLE=<table-name>
KUSTO_MAPPING=<avro-mapping-name>
```

Obtain a delegated Kusto access token for the signed-in Azure CLI user:

```powershell
$env:KUSTO_ACCESS_TOKEN = az account get-access-token `
  --resource "https://api.kusto.windows.net" `
  --query accessToken `
  --output tsv
```

The token is temporary. Generate a new token and restart the generator if it
expires during a long-running session. Do not print, commit, or persist the
token in a file.

Set the executable path:

```powershell
$exe = Join-Path $project "build-x64\Release\stock_ticker_generator.exe"
$envFile = Join-Path $repositoryRoot ".env"
```

## Streaming ingestion

Streaming mode sends each one-second batch directly to the Eventhouse query
service streaming-ingestion endpoint. This mode provides the lowest ingestion
latency.

Run continuously at 100 events per second:

```powershell
& $exe --mode streaming --env-file $envFile
```

Run for 30 seconds:

```powershell
& $exe --mode streaming --duration 30 --env-file $envFile
```

## Queued ingestion

Queued mode:

1. Gets a temporary ingestion container from the Eventhouse ingestion service.
2. Uploads the Avro batch to that container.
3. Submits the blob through the Kusto queued-ingestion REST endpoint.

Queued ingestion normally has higher latency than streaming ingestion, but it
is better suited to buffered ingestion and service-side batching.

Run continuously at 100 events per second:

```powershell
& $exe --mode queued --env-file $envFile
```

Run queued ingestion for 60 seconds at 200 events per second:

```powershell
& $exe --mode queued --rate 200 --duration 60 --env-file $envFile
```

The queued implementation uses the preview endpoint documented at:

<https://learn.microsoft.com/kusto/management/data-ingestion/queued-ingest-use-http?view=microsoft-fabric>

It does not implement the lower-level Azure Storage Queue protocol described
at:

<https://learn.microsoft.com/kusto/api/netfx/kusto-ingest-client-rest?view=microsoft-fabric>

## Command-line options

```text
stock_ticker_generator [--mode streaming|queued]
                       [--rate EVENTS_PER_SECOND]
                       [--duration SECONDS]
                       [--env-file PATH]
```

| Option | Default | Description |
| --- | --- | --- |
| `--mode` | `streaming` | Selects streaming or queued ingestion. |
| `--rate` | `100` | Number of events generated in each one-second batch. |
| `--duration` | Unlimited | Stops after the specified number of seconds. |
| `--env-file` | `.env` | Loads local configuration without overriding existing process environment variables. |
| `--help` | N/A | Displays command usage. |

Stop an unlimited run with `Ctrl+C`.

## Example output

Streaming:

```text
sent 100 events; total=100
sent 100 events; total=200
```

Queued:

```text
Queued operation <operation-id>: sent 100 events; total=100
```

## Verify in Eventhouse

Run this KQL query in the configured Eventhouse database:

```kusto
StockTicks
| where eventtime > ago(5m)
| summarize
    Events = count(),
    MinimumPrice = min(price),
    MaximumPrice = max(price)
    by ticker, bin(eventtime, 1s)
| order by eventtime desc, ticker asc
```

To see the latest individual events:

```kusto
StockTicks
| top 100 by eventtime desc
| project eventtime, ticker, price, eventname, eventdesc
```

## Source layout

| File | Purpose |
| --- | --- |
| `cpp/src/stock_ticker_generator.cpp` | Generates and schedules ticker events. |
| `cpp/src/stock_tick_producer.cpp` | Serializes Avro and performs streaming ingestion. |
| `cpp/src/queued_ingestion.cpp` | Uploads Avro and submits queued ingestion. |
| `cpp/src/environment_config.cpp` | Loads ignored local environment configuration. |
| `cpp/include/stock_tick_producer.h` | Streaming producer interface. |
| `cpp/include/queued_ingestion.h` | Queued producer interface. |
| `cpp/src/main.cpp` | Single-event demonstration executable. |
