#include "stock_tick_producer.h"

#include <fmt/format.h>
#include <avro/Compiler.hh>
#include <avro/DataFile.hh>
#include <avro/Encoder.hh>
#include <avro/Specific.hh>
#include <curl/curl.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <mutex>
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

void initialize_curl() {
    static std::once_flag flag;
    std::call_once(flag, [] {
        if (curl_global_init(CURL_GLOBAL_DEFAULT) != CURLE_OK) {
            throw std::runtime_error("curl_global_init failed");
        }
    });
}

std::string escape_url_component(CURL* curl, const std::string& value) {
    char* escaped = curl_easy_escape(
        curl, value.c_str(), static_cast<int>(value.size()));
    if (escaped == nullptr) {
        throw std::runtime_error("Failed to URL-encode ingestion parameter");
    }
    std::string result(escaped);
    curl_free(escaped);
    return result;
}

std::size_t append_response(
    char* data, std::size_t size, std::size_t count, void* destination) {
    const auto length = size * count;
    static_cast<std::string*>(destination)->append(data, length);
    return length;
}

}  // namespace

StockTick make_stock_tick(
    std::string ticker,
    double price,
    std::chrono::system_clock::time_point event_time) {
    if (ticker.empty()) {
        throw std::invalid_argument("ticker must not be empty");
    }
    if (price < 0.0) {
        throw std::invalid_argument("price must not be negative");
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

void ingest_stock_tick_avro(
    const StockTick& tick,
    const IngestConfig& config,
    const std::string& bearer_token) {
    ingest_stock_ticks_avro({tick}, config, bearer_token);
}

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

void ingest_stock_ticks_avro(
    const std::vector<StockTick>& ticks,
    const IngestConfig& config,
    const std::string& bearer_token) {
    if (config.streaming_ingest_uri.empty()) {
        throw std::invalid_argument(
            "streaming_ingest_uri must not be empty");
    }
    if (config.database.empty() || config.table.empty() ||
        config.mapping.empty()) {
        throw std::invalid_argument(
            "database, table, and mapping must not be empty");
    }
    if (bearer_token.empty()) {
        throw std::invalid_argument("bearer_token must not be empty");
    }

    initialize_curl();
    const std::vector<char> body = serialize_stock_ticks_avro(ticks);

    CURL* raw_curl = curl_easy_init();
    if (raw_curl == nullptr) {
        throw std::runtime_error("curl_easy_init failed");
    }
    std::unique_ptr<CURL, decltype(&curl_easy_cleanup)> curl(
        raw_curl, &curl_easy_cleanup);

    std::string base_uri = config.streaming_ingest_uri;
    while (!base_uri.empty() && base_uri.back() == '/') {
        base_uri.pop_back();
    }
    const std::string url =
        base_uri + "/v1/rest/ingest/" +
        escape_url_component(curl.get(), config.database) + "/" +
        escape_url_component(curl.get(), config.table) +
        "?streamFormat=Avro&mappingName=" +
        escape_url_component(curl.get(), config.mapping);

    curl_slist* raw_headers = nullptr;
    raw_headers = curl_slist_append(
        raw_headers, ("Authorization: Bearer " + bearer_token).c_str());
    raw_headers =
        curl_slist_append(raw_headers, "Content-Type: application/octet-stream");
    raw_headers = curl_slist_append(raw_headers, "Accept: application/json");
    raw_headers = curl_slist_append(raw_headers, "Expect:");
    raw_headers = curl_slist_append(raw_headers, "x-ms-app: TickPOCProducer");
    if (raw_headers == nullptr) {
        throw std::runtime_error("Unable to allocate HTTP headers");
    }
    std::unique_ptr<curl_slist, decltype(&curl_slist_free_all)> headers(
        raw_headers, &curl_slist_free_all);

    std::string response;
    curl_easy_setopt(curl.get(), CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl.get(), CURLOPT_HTTPHEADER, headers.get());
    curl_easy_setopt(curl.get(), CURLOPT_POST, 1L);
    curl_easy_setopt(curl.get(), CURLOPT_POSTFIELDS, body.data());
    curl_easy_setopt(
        curl.get(), CURLOPT_POSTFIELDSIZE_LARGE,
        static_cast<curl_off_t>(body.size()));
    curl_easy_setopt(curl.get(), CURLOPT_WRITEFUNCTION, append_response);
    curl_easy_setopt(curl.get(), CURLOPT_WRITEDATA, &response);
    curl_easy_setopt(curl.get(), CURLOPT_TIMEOUT, 30L);

    const CURLcode result = curl_easy_perform(curl.get());
    if (result != CURLE_OK) {
        throw std::runtime_error(
            std::string("Ingestion request failed: ") +
            curl_easy_strerror(result));
    }

    long status = 0;
    curl_easy_getinfo(curl.get(), CURLINFO_RESPONSE_CODE, &status);
    if (status < 200 || status >= 300) {
        throw std::runtime_error(
            "Ingestion returned HTTP " + std::to_string(status) +
            (response.empty() ? std::string{} : ": " + response));
    }
}

}  // namespace tickpoc
