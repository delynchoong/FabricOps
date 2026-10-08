# EventHubTickerProducer

This C++20 PoC publishes binary Apache Avro ticker events through Azure Event
Hubs into Microsoft Fabric Eventhouse. The recommended implementation is:

```text
one EventData message
└── one self-describing Avro OCF body
    ├── top-level Avro record
    │   ├── fixed primitive fields encoded in schema order
    │   └── variablefields Avro map
    └── one or more OCF data blocks
```

Use `eventhub_ticker_dynamic_map_producer` for the validated implementation.
It combines efficiently encoded fixed fields with arbitrary string map keys,
stores the map in a KQL `dynamic` column, and allows new keys to be queried
without changing the Eventhouse table or ingestion mapping.

## Validated implementation and PoC outcomes

| Scenario | Result | Status |
| --- | --- | --- |
| Complete Avro OCF with embedded writer schema | Rows ingested successfully | Recommended |
| Fixed outer fields in an Avro `record` | Field names stored once in the schema; values encoded in schema order | Recommended |
| `variablefields` Avro `map` to KQL `dynamic` | Arbitrary keys and mixed supported scalar values queried successfully | Recommended |
| Five records and five blocks in one EventData body | Eventhouse produced five rows | Validated |
| Per-message table routing within one KQL database | `Table` and `IngestionMappingReference` routed TAS/TAQ successfully | Supported |
| Raw Avro datum without OCF header/schema | Rejected with `wrong magic in header` | Unsupported negative test |
| `Database` property for another KQL database | Consumed but produced no rows in target or default database | Unsupported in Fabric |
| `DatabaseRouting=Multi` sent to the Fabric workload endpoint | Accepted but ignored | Unsupported in Fabric |

The unsupported executables remain in the project only to reproduce and
document the negative results. Do not use them as deployment templates.

## Recommended record and map schema

| Field | Avro type | Example |
| --- | --- | --- |
| `eventname` | string | `stock ticks` |
| `eventtime` | timestamp-millis | Current UTC time |
| `ticker` | string | `MSFT` |
| `price` | double | `425.75` |
| `eventdesc` | string | `stock ticker price` |
| `variablefields` | map with union values | `{"currency":"GBP"}` |

The top-level object is an Avro `record`. The first five fields are primitive
fields; they are not nested records or repeated key/value labels in the binary
record body. `variablefields` is an Avro map, so its dynamic key strings are
encoded whenever they are present.

## Producer inventory

| Executable | Purpose |
| --- | --- |
| `eventhub_ticker_dynamic_map_producer` | Recommended record-plus-map implementation |
| `eventhub_ticker_producer` | Validated fixed-record OCF producer |
| `eventhub_ticker_routing_producer` | Validated same-database table-routing test |
| `eventhub_ticker_dynamic_producer` | Fixed nested-record comparison test |
| `eventhub_ticker_raw_producer` | Intentional unsupported raw-datum test |
| `eventhub_ticker_cross_database_routing_producer` | Intentional unsupported Fabric cross-database test |

All successful Eventhouse ingestion paths send a complete OCF beginning with
`Obj\x01` and containing its writer schema.

## Deployed development resources

- Subscription: `10511ed0-f53c-407d-818a-b896a963299d`
- Resource group: `westus-rg-01`
- Region: West US
- Event Hubs namespace: `westus-ehns-01`
- Event Hub: `stock-ticks`
- Fabric workspace: `RTI_workspace`
- Fabric KQL database: `TickPOCEventhouse`

The namespace uses Standard SKU and TLS 1.2. The producer uses Microsoft Entra
authentication. The direct Eventhouse connection uses a listen-only SAS rule
because that connector doesn't use the workspace identity credential. A
resource-level policy exemption permits local authentication only for this
namespace through October 16, 2026.

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
through Azure CLI and keep credentials outside configuration files:

```powershell
az login --use-device-code --tenant 91a75bbf-0e1f-4ba2-89f6-be394394becc
az account set --subscription 10511ed0-f53c-407d-818a-b896a963299d

$env:EVENTHUBS_HOST = "westus-ehns-01.servicebus.windows.net"
$env:EVENTHUB_NAME = "stock-ticks"

& "C:\b\evtick\Release\eventhub_ticker_dynamic_map_producer.exe" `
  --print-message `
  --dump-avro ".\dynamic-map-five-blocks.avro"
```

This sends one EventData body containing five Avro records with different map
keys. Use the later KQL validation query to confirm five rows and inspect every
dynamic key and value type.

## Unsupported experiment: raw Avro datum without a schema

To test whether the direct Eventhouse Avro data connection can decode raw Avro
datum bytes without an object-container schema:

```powershell
$rawExe = "C:\b\evtick\Release\eventhub_ticker_raw_producer.exe"
& $rawExe --rate 5 --duration 3
```

> **Do not use this producer for production ingestion.** It intentionally
> omits the OCF header and embedded writer schema required by the validated
> Fabric Eventhouse connector.

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

`TickPOCEventhouse` reads `stock-ticks` in the replacement `westus-ehns-01`
namespace through this direct connection:

- Cloud connection: `westus-eventhub-sas-01`
- Cloud connection ID: `ad0694bf-94d4-449b-b0eb-b9c1497ad31e`
- Consumer group: `fabric-stream1`
- Target table: `StockTicks`
- Data format: `Avro`
- Ingestion mapping: `StockTicksAvroMapping`
- Direct data connection: `westus-ehdc-01`
- Direct data connection ID: `4cd08d3b-5bfc-4aff-a392-4ee782f36e0a`
- Event Hubs authorization rule: `FabricEventhouseListen` with `Listen` only

The direct connection was validated by publishing five Avro object-container
messages after its creation and confirming that all five landed in
`StockTicks`.

### Create the Avro ingestion mapping

Set the deployment values:

```powershell
$subscriptionId = "10511ed0-f53c-407d-818a-b896a963299d"
$resourceGroup = "westus-rg-01"
$namespaceName = "westus-ehns-01"
$eventHubName = "stock-ticks"
$consumerGroup = "fabric-stream1"
$sasRuleName = "FabricEventhouseListen"

$workspaceId = "3608a416-708e-4b84-8b89-99c334da219f"
$databaseId = "ef8be933-cefb-407e-a2e9-fd53eaedcc6b"
$databaseName = "TickPOCEventhouse"
$capacityId = "c12d00cc-f885-4644-867b-202885b17bc1"
$queryServiceUri = "https://trd-9dvvq6080wsmervjn8.z9.kusto.fabric.microsoft.com"
```

Create the table and named Avro ingestion mapping through the Kusto management
API:

```powershell
$kustoToken = az account get-access-token `
  --resource "https://kusto.kusto.windows.net" `
  --query accessToken `
  --output tsv

$tableCommand = @'
.create-merge table StockTicks (
    eventname: string,
    eventtime: datetime,
    ticker: string,
    price: real,
    eventdesc: string
)
'@

$mappingCommand = @'
.create-or-alter table StockTicks ingestion avro mapping
'StockTicksAvroMapping'
'[{"column":"eventname","path":"$.eventname"},
  {"column":"eventtime","path":"$.eventtime","transform":"DateTimeFromUnixMilliseconds"},
  {"column":"ticker","path":"$.ticker"},
  {"column":"price","path":"$.price"},
  {"column":"eventdesc","path":"$.eventdesc"}]'
'@

foreach ($command in @($tableCommand, $mappingCommand)) {
  $body = @{
    db = $databaseName
    csl = $command
  } | ConvertTo-Json -Compress

  Invoke-RestMethod `
    -Method Post `
    -Uri "$queryServiceUri/v1/rest/mgmt" `
    -Headers @{
      Authorization = "Bearer $kustoToken"
      "Content-Type" = "application/json"
    } `
    -Body $body
}
```

`StockTicksAvroMapping` converts the Avro `timestamp-millis` value to the
Eventhouse `datetime` column and maps the remaining Avro fields by name.

### Create the Fabric Event Hub cloud connection

Create the Fabric cloud connection with the listen-only Event Hubs SAS rule:

```powershell
$sasKey = az eventhubs eventhub authorization-rule keys list `
  --subscription $subscriptionId `
  --resource-group $resourceGroup `
  --namespace-name $namespaceName `
  --eventhub-name $eventHubName `
  --name $sasRuleName `
  --query primaryKey `
  --output tsv

$fabricToken = az account get-access-token `
  --resource "https://api.fabric.microsoft.com" `
  --query accessToken `
  --output tsv

$cloudConnectionBody = @{
  connectivityType = "ShareableCloud"
  displayName = "westus-eventhub-sas-01"
  connectionDetails = @{
    type = "EventHub"
    creationMethod = "EventHub.Contents"
    parameters = @(
      @{
        dataType = "Text"
        name = "endpoint"
        value = "$namespaceName.servicebus.windows.net"
      },
      @{
        dataType = "Text"
        name = "entityPath"
        value = $eventHubName
      }
    )
  }
  privacyLevel = "Organizational"
  credentialDetails = @{
    singleSignOnType = "None"
    connectionEncryption = "NotEncrypted"
    skipTestConnection = $false
    credentials = @{
      credentialType = "Basic"
      username = $sasRuleName
      password = $sasKey
    }
  }
} | ConvertTo-Json -Depth 10

$cloudConnection = Invoke-RestMethod `
  -Method Post `
  -Uri "https://api.fabric.microsoft.com/v1/connections" `
  -Headers @{
    Authorization = "Bearer $fabricToken"
    "Content-Type" = "application/json"
  } `
  -Body $cloudConnectionBody

$cloudConnectionId = $cloudConnection.id
```

The SAS key remains in process memory and is stored in the Fabric connection.
It is not added to source control or application configuration.

For the deployed connection, the existing ID is:

```powershell
$cloudConnectionId = "ad0694bf-94d4-449b-b0eb-b9c1497ad31e"
```

### Create the direct Avro data connection

Request a Kusto workload token:

```powershell
$powerBiToken = az account get-access-token `
  --resource "https://analysis.windows.net/powerbi/api" `
  --query accessToken `
  --output tsv

$mwcTokenBody = @{
  type = "[Start] GetMWCTokenV2"
  workloadType = "Kusto"
  artifactObjectIds = @($databaseId)
  workspaceObjectId = $workspaceId
  capacityObjectId = $capacityId
} | ConvertTo-Json -Depth 5

$mwcTokenResponse = Invoke-RestMethod `
  -Method Post `
  -Uri "https://wabi-us-central-b-primary-redirect.analysis.windows.net/metadata/v201606/generatemwctokenv2" `
  -Headers @{
    Authorization = "Bearer $powerBiToken"
    "Content-Type" = "application/json"
  } `
  -Body $mwcTokenBody

$mwcToken = $mwcTokenResponse.Token
```

Create the Eventhouse data connection with `DataFormat` set to `Avro`:

```powershell
$capacityIdCompact = $capacityId -replace "-", ""
$dataConnectionUrl = `
  "https://$capacityIdCompact.pbidedicated.windows.net" +
  "/webapi/capacities/$capacityId" +
  "/workloads/Kusto/KustoService/direct/v1" +
  "/databases/$databaseId" +
  "/dataConnections/$cloudConnectionId"

$dataConnectionBody = @{
  DataConnectionType = "EventHubDataConnection"
  DataConnectionProperties = @{
    DatabaseArtifactId = $databaseId
    TableName = "StockTicks"
    MappingRuleName = "StockTicksAvroMapping"
    EventSystemProperties = @()
    ConsumerGroup = $consumerGroup
    Compression = "None"
    DataFormat = "Avro"
    DataSourceConnectionId = $cloudConnectionId
    DataConnectionType = "EventHubDataConnection"
    DataConnectionName = "westus-ehdc-01"
  }
} | ConvertTo-Json -Depth 10

$dataConnection = Invoke-RestMethod `
  -Method Post `
  -Uri $dataConnectionUrl `
  -Headers @{
    Authorization = "MwcToken $mwcToken"
    "Content-Type" = "application/json"
  } `
  -Body $dataConnectionBody

$dataConnection.dataSourceConnectionId
```

The deployed data connection ID is:

```text
4cd08d3b-5bfc-4aff-a392-4ee782f36e0a
```

Verify recent records in the target KQL table:

```kusto
StockTicks
| where eventtime > ago(5m)
| summarize Events=count(), MinPrice=min(price), MaxPrice=max(price)
    by ticker, bin(eventtime, 1s)
| order by eventtime desc, ticker asc
```

## Nested Avro records in a dynamic column

`eventhub_ticker_dynamic_producer` validates that the Eventhouse direct Avro
connection can decode a nested Avro `record` and store it in a KQL `dynamic`
column. The outer record retains the stock-tick fields and adds
`variablefields`, whose schema contains `variablefield1` through
`variablefield10`. Each nested field uses this union:

```json
["null", "string", "boolean"]
```

The producer populates a different number of nested values in each event:
the first event has one non-null field, the second has two, and the tenth has
all ten. String and boolean values alternate. The complete nested schema is
embedded in each Avro object container.

Create the table and Avro mapping:

```kusto
.create-merge table DynamicTicks (
    eventname: string,
    eventtime: datetime,
    ticker: string,
    price: real,
    eventdesc: string,
    variablefields: dynamic
)

.create-or-alter table DynamicTicks ingestion avro mapping
'DynamicTicksAvroMapping'
'[{"column":"eventname","path":"$.eventname"},
  {"column":"eventtime","path":"$.eventtime","transform":"DateTimeFromUnixMilliseconds"},
  {"column":"ticker","path":"$.ticker"},
  {"column":"price","path":"$.price"},
  {"column":"eventdesc","path":"$.eventdesc"},
  {"column":"variablefields","path":"$.variablefields"}]'
```

Build and run the dedicated producer:

```powershell
& "C:\b\evtick\Release\eventhub_ticker_dynamic_producer.exe" `
  --count 1 `
  --batch-size 1 `
  --print-message `
  --dump-avro ".\dynamic-tick.avro"
```

`--print-message` prints the logical Avro record, EventData routing properties,
OCF byte count, and OCF magic header before the event is added to the batch.
The EventData body remains binary Avro; the JSON-shaped line is a readable
representation of the values encoded in that body. `--dump-avro` writes the
exact byte vector assigned to `EventData.Body`, reads it back, and decodes it
with Avro C++ before the event is sent. It requires `--count 1` so the dump
corresponds to exactly one EventData message.

Example terminal output:

```text
Avro logical record: {"eventname":"dynamic stock ticks","eventtime":1790897676560,"ticker":"DYN1","price":238.46,"eventdesc":"nested Avro record to KQL dynamic column","variablefields":{"variablefield1":"value-0-1","variablefield2":null,"variablefield3":null,"variablefield4":null,"variablefield5":null,"variablefield6":null,"variablefield7":null,"variablefield8":null,"variablefield9":null,"variablefield10":null}}
EventData properties: Table=DynamicTicks, Format=Avro, IngestionMappingReference=DynamicTicksAvroMapping, Compression=None
Avro OCF body: bytes=1329, magic=4f 62 6a 01 (Obj\x01)
EventData.Body hex[0..31]: 4f 62 6a 01 04 14 61 76 72 6f 2e 63 6f 64 65 63 08 6e 75 6c 6c 16 61 76 72 6f 2e 73 63 68 65 6d
Wrote exact EventData.Body to .\dynamic-tick.avro and decoded it successfully as Avro OCF
Queued dynamic event: ticker=DYN1, populatedFields=1, price=238.46
Sent batch 1: events=1, totalEvents=1
Completed dynamic publish: eventsSent=1, batchesSent=1
```

Inspect the exact transmitted body:

```powershell
Format-Hex -Path ".\dynamic-tick.avro" | Select-Object -First 2
Get-FileHash -Algorithm SHA256 ".\dynamic-tick.avro"
```

The first four bytes must be `4F 62 6A 01`. A JSON object would start with
`7B` (`{`), so this check proves that the body is Avro OCF rather than the
JSON-shaped diagnostic line.

Each EventData message overrides the static connection target with:

```text
Table=DynamicTicks
Format=Avro
IngestionMappingReference=DynamicTicksAvroMapping
Compression=None
```

Query nested fields directly from the dynamic column:

```kusto
DynamicTicks
| top 10 by eventtime desc
| project
    eventtime,
    ticker,
    variablefields,
    variablefield1 = variablefields.variablefield1,
    variablefield1Type = gettype(variablefields.variablefield1),
    variablefield2 = variablefields.variablefield2,
    variablefield2Type = gettype(variablefields.variablefield2),
    variablefield10 = variablefields.variablefield10,
    variablefield10Type = gettype(variablefields.variablefield10)
| order by ticker asc
```

The deployment was validated with three ten-event batches. All 30 events
landed in `DynamicTicks`. The latest batch produced this non-null field
distribution:

| Ticker | Populated fields | String fields | Boolean fields |
| --- | ---: | ---: | ---: |
| `DYN1` | 1 | 1 | 0 |
| `DYN2` | 2 | 1 | 1 |
| `DYN3` | 3 | 2 | 1 |
| `DYN4` | 4 | 2 | 2 |
| `DYN5` | 5 | 3 | 2 |
| `DYN6` | 6 | 3 | 3 |
| `DYN7` | 7 | 4 | 3 |
| `DYN8` | 8 | 4 | 4 |
| `DYN9` | 9 | 5 | 4 |
| `DYN10` | 10 | 5 | 5 |

Avro records have a fixed field schema. Therefore, fields that aren't
populated are present in the Eventhouse dynamic object with a `null` value;
the number of non-null values varies by event. Use an Avro `map` instead when
field names themselves must be unknown or physically absent at serialization
time.

## Variable Avro map fields in a dynamic column

`eventhub_ticker_dynamic_map_producer` uses an Avro `map` when each record
needs a different set of field names. The map keys are arbitrary strings, and
each value selects one branch from this shared value schema:

```json
{
  "name": "variablefields",
  "type": {
    "type": "map",
    "values": [
      "null",
      "string",
      "boolean",
      "long",
      "double"
    ]
  },
  "default": {}
}
```

The producer writes five records into one Avro OCF body. It flushes after
records one through four and closes after record five, producing exactly five
non-empty Avro data blocks without an extra empty block. That complete binary
OCF is sent as one EventData message and becomes five rows in `DynamicTicks`.

Build and run the map test:

```powershell
$env:EVENTHUBS_HOST = "westus-ehns-01.servicebus.windows.net"
$env:EVENTHUB_NAME = "stock-ticks"

& "C:\b\evtick\Release\eventhub_ticker_dynamic_map_producer.exe" `
  --print-message `
  --dump-avro ".\dynamic-map-five-blocks.avro"
```

The producer prints each logical record for inspection, verifies that the
exact dumped `EventData.Body` decodes back to the five source records, checks
that it contains five data blocks, and confirms the binary `Obj\x01` OCF
header before sending. Serialization and local verification use Avro memory
streams, so concurrent producers don't share temporary files. The block
validator parses OCF metadata, block counts, block sizes, and sync markers
rather than searching record bytes for marker-like sequences.

Query the resulting property bags and their KQL value types:

```kusto
DynamicTicks
| where eventname == "dynamic Avro map ticks"
| top 5 by eventtime desc
| extend fieldNames=bag_keys(variablefields)
| mv-apply fieldName=fieldNames on (
    extend
        fieldName=tostring(fieldName),
        fieldValue=variablefields[tostring(fieldName)],
        fieldType=gettype(variablefields[tostring(fieldName)])
    | summarize fields=make_list(
        pack("name", fieldName, "value", fieldValue, "type", fieldType))
)
| project
    eventtime,
    ticker,
    eventdesc,
    keyCount=array_length(fieldNames),
    variablefields,
    fields
| order by ticker asc
```

The live Fabric Eventhouse test produced:

| Ticker | Map keys | Key count | KQL value types |
| --- | --- | ---: | --- |
| `MAP1` | `venue` | 1 | `string` |
| `MAP2` | `bid`, `isIndicative` | 2 | `double`, `bool` |
| `MAP3` | `currency`, `tradePrice`, `tradeSize` | 3 | `string`, `double`, `long` |
| `MAP4` | `auctionType`, `imbalance`, `isClosingAuction`, `matchedVolume` | 4 | `string`, `double`, `bool`, `long` |
| `MAP5` | `condition`, `isCorrection`, `note`, `sequenceNumber`, `yield` | 5 | `string`, `bool`, `null`, `long`, `double` |

Unlike a nested Avro `record`, absent map keys are not serialized as fixed
fields with null values. Records and OCF data blocks can contain different map
key names, while all values must still conform to the common union declared in
the single writer schema embedded in the OCF header.

The ingestion mapping maps the complete Avro map to one KQL `dynamic` column;
it doesn't need an entry for every map key. New keys are immediately
queryable:

```kusto
DynamicTicks
| where ticker == "MAP3"
| top 1 by eventtime desc
| project
    currency=tostring(variablefields.currency),
    tradePrice=todouble(variablefields.tradePrice),
    tradeSize=tolong(variablefields.tradeSize)
```

## Per-event table routing

`eventhub_ticker_routing_producer` sends TAS and TAQ events through the same
Event Hub and `fabric-stream1` consumer group. Each EventData message contains
one Avro object container and these case-sensitive application properties:

| Property | TAS value | TAQ value |
| --- | --- | --- |
| `Table` | `TASTicks` | `TAQTicks` |
| `Format` | `Avro` | `Avro` |
| `IngestionMappingReference` | `TASTicksAvroMapping` | `TAQTicksAvroMapping` |
| `Compression` | `None` | `None` |

The `Table` and `IngestionMappingReference` properties override the static
`StockTicks` target and mapping on `westus-ehdc-01` for that message. The
`feed` property is diagnostic metadata and does not control routing.

Create the routing targets and mappings through the Kusto management API:

```powershell
$mapping = @'
[{"column":"eventname","path":"$.eventname"},
  {"column":"eventtime","path":"$.eventtime","transform":"DateTimeFromUnixMilliseconds"},
  {"column":"ticker","path":"$.ticker"},
  {"column":"price","path":"$.price"},
  {"column":"eventdesc","path":"$.eventdesc"}]
'@

$commands = @(
  ".create-merge table TASTicks (eventname:string, eventtime:datetime, ticker:string, price:real, eventdesc:string)",
  ".create-or-alter table TASTicks ingestion avro mapping 'TASTicksAvroMapping' '$mapping'",
  ".create-merge table TAQTicks (eventname:string, eventtime:datetime, ticker:string, price:real, eventdesc:string)",
  ".create-or-alter table TAQTicks ingestion avro mapping 'TAQTicksAvroMapping' '$mapping'"
)

foreach ($command in $commands) {
  $body = @{
    db = $databaseName
    csl = $command
  } | ConvertTo-Json -Compress

  Invoke-RestMethod `
    -Method Post `
    -Uri "$queryServiceUri/v1/rest/mgmt" `
    -Headers @{
      Authorization = "******"
      "Content-Type" = "application/json"
    } `
    -Body $body
}
```

Publish an alternating TAS/TAQ batch:

```powershell
$env:EVENTHUBS_HOST = "westus-ehns-01.servicebus.windows.net"
$env:EVENTHUB_NAME = "stock-ticks"

& "C:\b\evtick\Release\eventhub_ticker_routing_producer.exe" `
  --count 10 `
  --batch-size 10
```

The ten-message batch produces five TAS rows in `TASTicks` and five TAQ rows
in `TAQTicks`. No routed rows are written to the static `StockTicks` target.

```kusto
union withsource=SourceTable TASTicks, TAQTicks
| summarize
    Events=count(),
    EventNames=make_set(eventname),
    Tickers=make_set(ticker)
    by SourceTable
| order by SourceTable asc
```

## Unsupported experiment: cross-database routing

> **Do not use this producer as a Fabric routing pattern.** It is retained to
> reproduce the missing multi-database connection capability. Use one static
> Event Hub/data connection per destination database, or route and ingest with
> a custom consumer.

The Eventhouse contains two additional read-write KQL databases for validating
the ADX `Database` ingestion property:

| Database | Fabric item ID | Table | Mapping |
| --- | --- | --- | --- |
| `TASDatabase` | `ed26acfe-b175-41d4-b87a-28d20a20ffd4` | `StockTicks` | `StockTicksAvroMapping` |
| `TAQDatabase` | `fe88952c-6a1a-4c02-bfd5-07565f159149` | `StockTicks` | `StockTicksAvroMapping` |

`eventhub_ticker_cross_database_routing_producer` attaches these properties:

```text
TAS:
Database=TASDatabase
Table=StockTicks
Format=Avro
IngestionMappingReference=StockTicksAvroMapping
Compression=None

TAQ:
Database=TAQDatabase
Table=StockTicks
Format=Avro
IngestionMappingReference=StockTicksAvroMapping
Compression=None
```

Build and run the producer:

```powershell
& "C:\b\evtick\Release\eventhub_ticker_cross_database_routing_producer.exe" `
  --count 10 `
  --batch-size 10
```

Fabric consumed the messages from `fabric-stream1`, but no rows were written to
`TASDatabase.StockTicks`, `TAQDatabase.StockTicks`, or the static
`TickPOCEventhouse.StockTicks` target. The Fabric workload API accepted both
`DatabaseRouting=Multi` and an intentionally invalid routing value, proving
that the property is ignored by the current API contract. Fabric documentation
also exposes only a database-scoped Event Hubs connection and provides no
multi-database routing setting.

The current direct Eventhouse connection therefore supports per-message table,
format, and mapping overrides within `TickPOCEventhouse`, but it does not
support the ADX multi-database connection mode. The cross-database producer
and target databases remain available for retesting when Fabric exposes that
mode.

## References

- <https://learn.microsoft.com/azure/event-hubs/event-hubs-about>
- <https://github.com/Azure/azure-sdk-for-cpp/tree/main/sdk/eventhubs/azure-messaging-eventhubs>
- <https://learn.microsoft.com/fabric/real-time-intelligence/event-streams/add-source-azure-event-hubs>
- <https://learn.microsoft.com/fabric/real-time-intelligence/get-data-event-hub>
- <https://learn.microsoft.com/rest/api/fabric/eventstream/topology/get-eventstream-source>
- <https://learn.microsoft.com/rest/api/fabric/core/connections/create-connection>
- <https://learn.microsoft.com/kusto/management/create-ingestion-mapping-command>
- <https://learn.microsoft.com/azure/data-explorer/ingest-data-event-hub-overview>
