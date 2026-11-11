# EventHubTickerProducer

This C++20 sample generates the same simulated stock-ticker events as
`EventhouseKustoIngest`, serializes each event as Apache Avro binary, and sends
the binary event body to Azure Event Hubs.

## Event schema

| Field | Avro type | Example |
| --- | --- | --- |
| `eventname` | string | `stock ticks` |
| `eventtime` | timestamp-millis | Current UTC time |
| `ticker` | string | `MSFT` |
| `price` | double | `425.75` |
| `eventdesc` | string | `stock ticker price` |

The project builds two producers:

- `eventhub_ticker_producer` sends a self-describing Avro object container
  with exactly one record. The AMQP content type is `avro/binary`.
- `eventhub_ticker_raw_producer` sends only the Avro binary-encoded record
  datum, without the `Obj\x01` container header, embedded schema, metadata, or
  sync marker. Its AMQP content type is `application/octet-stream`.

Both set the `avro.schema.name` application property to `tickpoc.StockTick`.

## Local deployment configuration

Copy the repository's [environment template](../.env.example) to the ignored
root `.env` and supply your own deployment values:

| Setting | Environment variable |
| --- | --- |
| Subscription | `AZURE_SUBSCRIPTION_ID` |
| Tenant | `AZURE_TENANT_ID` |
| Resource group | `AZURE_RESOURCE_GROUP` |
| Event Hubs namespace hostname | `EVENTHUBS_HOST` |
| Event Hub | `EVENTHUB_NAME` |
| Fabric workspace | `FABRIC_WORKSPACE_NAME`, `FABRIC_WORKSPACE_ID` |
| Fabric KQL database | `FABRIC_KQL_DATABASE` |

Use Standard SKU or above and TLS 1.2. The producer uses Microsoft Entra
authentication. If the direct Eventhouse connector requires SAS, use a
listen-only rule and obtain any required Azure Policy approval for your own
namespace. Do not copy another deployment's policy exemptions.

## Build

```powershell
$project = Join-Path (Get-Location) "EventHubTickerProducer\cpp"
$vcpkgRoot = Join-Path $env:USERPROFILE "vcpkg"
$cmake = "C:\Program Files (x86)\Microsoft Visual Studio\2022\BuildTools\Common7\IDE\CommonExtensions\Microsoft\CMake\CMake\bin\cmake.exe"

& $cmake `
  -S $project `
  -B "C:\b\evtick" `
  -G "Visual Studio 17 2022" `
  -A x64 `
  -DCMAKE_TOOLCHAIN_FILE="$vcpkgRoot\scripts\buildsystems\vcpkg.cmake" `
  -DVCPKG_TARGET_TRIPLET=x64-windows `
  -DVCPKG_HOST_TRIPLET=x64-windows

& $cmake --build "C:\b\evtick" --config Release
& $cmake --build "C:\b\evtick" --config Release --target RUN_TESTS
```

The short build path avoids Windows path-length failures while vcpkg installs
the Event Hubs SDK and its transitive dependencies.

## Authentication and run

The producer uses `DefaultAzureCredential`. For local development, sign in
through Azure CLI and keep credentials outside configuration files. From the
repository root, first run the environment-loading block in the root
[Authentication instructions](../README.md#authentication), then:

```powershell
foreach ($name in 'AZURE_TENANT_ID', 'AZURE_SUBSCRIPTION_ID', 'EVENTHUBS_HOST', 'EVENTHUB_NAME') {
  $value = [Environment]::GetEnvironmentVariable($name, 'Process')
  if ([string]::IsNullOrWhiteSpace($value) -or $value.Contains('<')) {
    throw "$name is required; replace its placeholder in the environment file."
  }
}
az login --use-device-code --tenant $env:AZURE_TENANT_ID
az account set --subscription $env:AZURE_SUBSCRIPTION_ID

$exe = "C:\b\evtick\Release\eventhub_ticker_producer.exe"
& $exe --rate 100 --duration 30
```

To test whether the direct Eventhouse Avro data connection can decode raw Avro
datum bytes without an object-container schema:

```powershell
$rawExe = "C:\b\evtick\Release\eventhub_ticker_raw_producer.exe"
& $rawExe --rate 5 --duration 3
```

The raw producer is an intentional negative compatibility test. The direct
Eventhouse connection was tested with five raw datum messages:

- Event Hubs accepted all five messages.
- No rows landed in `StockTicks`.
- Eventhouse reported `BadRequest_InvalidBlob: wrong magic in header`.
- A subsequent object-container message landed successfully, confirming that
  the failed raw batch doesn't stop the connection.

The result shows that this direct `DataFormat=Avro` connection expects an Avro
object container with the `Obj\x01` magic header. Supplying only the binary
record datum isn't sufficient, even when the ingestion mapping and schema name
are known.

Inspect the failure after running the raw producer:

```kusto
.show ingestion failures
| where FailedOn > ago(10m)
| where Table == "StockTicks"
| project FailedOn, ErrorCode, Details
| order by FailedOn desc
```

For an Azure-hosted production process, assign a managed identity the Azure
Event Hubs Data Sender role at the Event Hub scope. `DefaultAzureCredential`
will use that identity without code changes.

## Fabric ingestion

Configure the KQL database and Event Hub specified in `.env` to connect
directly without an Eventstream. Keep connection metadata local as well:

- Cloud connection: `FABRIC_CLOUD_CONNECTION_NAME`
- Cloud connection ID: `FABRIC_CLOUD_CONNECTION_ID`
- Consumer group: `FABRIC_CONSUMER_GROUP`
- Target table: `StockTicks`
- Data format: `Avro`
- Ingestion mapping: `StockTicksAvroMapping`
- Direct data connection ID: `FABRIC_DATA_CONNECTION_ID`
- Event Hubs authorization rule: `FABRIC_EVENTHUB_AUTH_RULE` with `Listen` only

These metadata variables document the destination; the producer itself only
reads `EVENTHUBS_HOST` and `EVENTHUB_NAME`. A live validation published 10 Avro
object-container messages and confirmed that all 10 landed in `StockTicks`.

The Fabric Get Data documentation doesn't list Avro in the direct Event Hubs
format dropdown, but the underlying Kusto direct data-connection API accepts
`DataFormat=Avro`. This deployment verifies that the setting works with the
one-record Avro object containers produced by this application and the existing
Avro ingestion mapping.

Verify recent records in the target KQL table:

```kusto
StockTicks
| where eventtime > ago(5m)
| summarize Events=count(), MinPrice=min(price), MaxPrice=max(price)
    by ticker, bin(eventtime, 1s)
| order by eventtime desc, ticker asc
```

## References

- <https://learn.microsoft.com/azure/event-hubs/event-hubs-about>
- <https://github.com/Azure/azure-sdk-for-cpp/tree/main/sdk/eventhubs/azure-messaging-eventhubs>
- <https://learn.microsoft.com/fabric/real-time-intelligence/event-streams/add-source-azure-event-hubs>
- <https://learn.microsoft.com/fabric/real-time-intelligence/get-data-event-hub>
- <https://learn.microsoft.com/rest/api/fabric/eventstream/topology/get-eventstream-source>
