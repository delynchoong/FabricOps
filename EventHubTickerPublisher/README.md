# EventHubTickerPublisher

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

The project builds two publishers:

- `eventhub_ticker_publisher` sends a self-describing Avro object container
  with exactly one record. The AMQP content type is `avro/binary`.
- `eventhub_ticker_raw_publisher` sends only the Avro binary-encoded record
  datum, without the `Obj\x01` container header, embedded schema, metadata, or
  sync marker. Its AMQP content type is `application/octet-stream`.

Both set the `avro.schema.name` application property to `tickpoc.StockTick`.

## Deployed development resources

- Subscription: `10511ed0-f53c-407d-818a-b896a963299d`
- Resource group: `westus-rg-01`
- Region: West US
- Event Hubs namespace: `tickpoc-ehns-delyn`
- Event Hub: `stock-ticks`
- Fabric workspace: `RTI_workspace`
- Fabric KQL database: `TickPOCEventhouse`

The namespace uses Standard SKU and TLS 1.2. The publisher uses Microsoft Entra
authentication. The direct Eventhouse connection uses a listen-only SAS rule
because that connector doesn't use the workspace identity credential. A
resource-level policy exemption permits local authentication only for this
namespace through October 16, 2026.

## Build

```powershell
$project = Join-Path (Get-Location) "EventHubTickerPublisher\cpp"
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

The publisher uses `DefaultAzureCredential`. For local development, sign in
through Azure CLI and keep credentials outside configuration files:

```powershell
az login --use-device-code --tenant 91a75bbf-0e1f-4ba2-89f6-be394394becc
az account set --subscription 10511ed0-f53c-407d-818a-b896a963299d

$env:EVENTHUBS_HOST = "tickpoc-ehns-delyn.servicebus.windows.net"
$env:EVENTHUB_NAME = "stock-ticks"
$exe = "C:\b\evtick\Release\eventhub_ticker_publisher.exe"
& $exe --rate 100 --duration 30
```

To test whether the direct Eventhouse Avro data connection can decode raw Avro
datum bytes without an object-container schema:

```powershell
$rawExe = "C:\b\evtick\Release\eventhub_ticker_raw_publisher.exe"
& $rawExe --rate 5 --duration 3
```

The raw publisher is an intentional negative compatibility test. The direct
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

Inspect the failure after running the raw publisher:

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

`TickPOCEventhouse` reads `stock-ticks` directly without an Eventstream:

- Cloud connection: `TickPOC Event Hub Direct SAS`
- Cloud connection ID: `64577c99-1ceb-4d5b-9200-f2841237501c`
- Consumer group: `fabric-eventstream`
- Target table: `StockTicks`
- Data format: `Avro`
- Ingestion mapping: `StockTicksAvroMapping`
- Direct data connection ID: `b6e1a725-e7f5-4b65-b6e5-f86a48502ce2`
- Event Hubs authorization rule: `FabricEventhouseListen` with `Listen` only

The previous `TickPOCEventHubIngest` Eventstream was deleted. A live validation
published 10 Avro object-container messages and confirmed that all 10 landed in
`StockTicks`.

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
