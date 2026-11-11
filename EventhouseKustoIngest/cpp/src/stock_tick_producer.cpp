#include "stock_tick_ingestion.h"

#include "http_client.h"
#include "stock_tick_serialization.h"

// Avro C++ 1.12.1 uses fmt::format without including fmt/format.h.
#include <fmt/format.h>
#include <avro/Compiler.hh>
#include <avro/DataFile.hh>
#include <avro/Encoder.hh>
#include <avro/Specific.hh>

#include <chrono>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <sstream>
#include <stdexcept>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

namespace avro {

template <>
struct codec_traits<tickpoc::StockTick> {
    static void encode(Encoder& encoder, const tickpoc::StockTick& value) {
        avro::encode(encoder, value.eventname);
        avro::encode(encoder, value.eventtime);
        avro::encode(encoder, value.ticker);
        avro::encode(encoder, value.price);
        avro::encode(encoder, value.eventdesc);
    }

    static void decode(Decoder& decoder, tickpoc::StockTick& value) {
        avro::decode(decoder, value.eventname);
        avro::decode(decoder, value.eventtime);
        avro::decode(decoder, value.ticker);
        avro::decode(decoder, value.price);
        avro::decode(decoder, value.eventdesc);
    }
};

}  // namespace avro

namespace tickpoc {
namespace {

constexpr std::string_view kAvroSchema = R"({
  "type": "record",
  "name": "StockTick",
  "namespace": "tickpoc",
  "fields": [
    {"name": "eventname", "type": "string"},
    {"name": "eventtime", "type": "long"},
    {"name": "ticker", "type": "string"},
    {"name": "price", "type": "double"},
    {"name": "eventdesc", "type": "string"}
  ]
})";

class TemporaryFile {
public:
    TemporaryFile() {
        const auto nonce = std::chrono::steady_clock::now()
                               .time_since_epoch()
                               .count();
        path_ = std::filesystem::temp_directory_path() /
                ("tickpoc-" + std::to_string(nonce) + ".avro");
    }

    ~TemporaryFile() {
        std::error_code error;
        std::filesystem::remove(path_, error);
    }

    const std::filesystem::path& path() const { return path_; }

private:
    std::filesystem::path path_;
};

}  // namespace

StockTick make_stock_tick(
    std::string ticker,
    double price,
    std::chrono::system_clock::time_point event_time) {
    if (ticker.empty()) {
        throw std::invalid_argument("ticker must not be empty");
    }
    if (!std::isfinite(price) || price < 0.0) {
        throw std::invalid_argument(
            "price must be finite and must not be negative");
    }

    return StockTick{
        .eventname = "stock ticks",
        .eventtime = std::chrono::duration_cast<std::chrono::milliseconds>(
                         event_time.time_since_epoch())
                         .count(),
        .ticker = std::move(ticker),
        .price = price,
        .eventdesc = "stock ticker price",
    };
}

void ingest_stock_tick_streaming(
    const StockTick& tick,
    const StreamingIngestConfig& config,
    const std::string& bearer_token) {
    ingest_stock_ticks_streaming({tick}, config, bearer_token);
}

namespace detail {

std::vector<char> serialize_stock_ticks_avro(
    const std::vector<StockTick>& ticks) {
    if (ticks.empty()) {
        throw std::invalid_argument("ticks must not be empty");
    }

    std::istringstream schema_stream{std::string(kAvroSchema)};
    avro::ValidSchema schema;
    avro::compileJsonSchema(schema_stream, schema);

    TemporaryFile file;
    {
        avro::DataFileWriter<StockTick> writer(
            file.path().string().c_str(), schema);
        for (const auto& tick : ticks) {
            writer.write(tick);
        }
        writer.close();
    }

    std::ifstream input(file.path(), std::ios::binary);
    if (!input) {
        throw std::runtime_error("Unable to read generated Avro file");
    }
    return {std::istreambuf_iterator<char>(input),
            std::istreambuf_iterator<char>()};
}

void validate_ingestion_target(const IngestionTarget& target) {
    if (target.database.empty() || target.table.empty() ||
        target.mapping.empty()) {
        throw std::invalid_argument(
            "database, table, and mapping must not be empty");
    }
}

}  // namespace detail

void ingest_stock_ticks_streaming(
    const std::vector<StockTick>& ticks,
    const StreamingIngestConfig& config,
    const std::string& bearer_token) {
    if (config.query_uri.empty()) {
        throw std::invalid_argument("query_uri must not be empty");
    }
    detail::validate_ingestion_target(config.target);
    if (bearer_token.empty()) {
        throw std::invalid_argument("bearer_token must not be empty");
    }

    const std::vector<char> body =
        detail::serialize_stock_ticks_avro(ticks);

    const std::string url =
        detail::trim_trailing_slashes(config.query_uri) +
        "/v1/rest/ingest/" +
        detail::url_encode(config.target.database) + "/" +
        detail::url_encode(config.target.table) +
        "?streamFormat=Avro&mappingName=" +
        detail::url_encode(config.target.mapping);
    const std::string request_id =
        "EventhouseKustoIngest.Streaming;" + detail::make_guid();
    const auto response = detail::send_http_request(
        "POST",
        url,
        {
            "Authorization: Bearer " + bearer_token,
            "Content-Type: application/octet-stream",
            "Accept: application/json",
            "Expect:",
            "x-ms-app: EventhouseKustoIngest",
            "x-ms-client-version: 1.0.0",
            "x-ms-client-request-id: " + request_id,
        },
        body,
        30L);
    detail::require_http_success(response, "Streaming ingestion");
}

}  // namespace tickpoc
