#include "queued_ingestion.h"

#include <curl/curl.h>
#include <nlohmann/json.hpp>

#include <chrono>
#include <iomanip>
#include <memory>
#include <mutex>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace tickpoc {
namespace {

struct HttpResponse {
    long status;
    std::string body;
};

void initialize_curl() {
    static std::once_flag flag;
    std::call_once(flag, [] {
        if (curl_global_init(CURL_GLOBAL_DEFAULT) != CURLE_OK) {
            throw std::runtime_error("curl_global_init failed");
        }
    });
}

std::size_t append_response(
    char* data, std::size_t size, std::size_t count, void* destination) {
    const auto length = size * count;
    static_cast<std::string*>(destination)->append(data, length);
    return length;
}

HttpResponse send_request(
    const std::string& method,
    const std::string& url,
    const std::vector<std::string>& header_values,
    const std::vector<char>& body) {
    CURL* raw_curl = curl_easy_init();
    if (raw_curl == nullptr) {
        throw std::runtime_error("curl_easy_init failed");
    }
    std::unique_ptr<CURL, decltype(&curl_easy_cleanup)> curl(
        raw_curl, &curl_easy_cleanup);

    curl_slist* raw_headers = nullptr;
    for (const auto& value : header_values) {
        curl_slist* next = curl_slist_append(raw_headers, value.c_str());
        if (next == nullptr) {
            curl_slist_free_all(raw_headers);
            throw std::runtime_error("Unable to allocate HTTP headers");
        }
        raw_headers = next;
    }
    std::unique_ptr<curl_slist, decltype(&curl_slist_free_all)> headers(
        raw_headers, &curl_slist_free_all);

    HttpResponse response{};
    curl_easy_setopt(curl.get(), CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl.get(), CURLOPT_CUSTOMREQUEST, method.c_str());
    curl_easy_setopt(curl.get(), CURLOPT_HTTPHEADER, headers.get());
    curl_easy_setopt(curl.get(), CURLOPT_POSTFIELDS, body.data());
    curl_easy_setopt(
        curl.get(), CURLOPT_POSTFIELDSIZE_LARGE,
        static_cast<curl_off_t>(body.size()));
    curl_easy_setopt(curl.get(), CURLOPT_WRITEFUNCTION, append_response);
    curl_easy_setopt(curl.get(), CURLOPT_WRITEDATA, &response.body);
    curl_easy_setopt(curl.get(), CURLOPT_TIMEOUT, 60L);

    const CURLcode result = curl_easy_perform(curl.get());
    if (result != CURLE_OK) {
        throw std::runtime_error(
            std::string("HTTP request failed: ") +
            curl_easy_strerror(result));
    }
    curl_easy_getinfo(curl.get(), CURLINFO_RESPONSE_CODE, &response.status);
    return response;
}

void require_success(
    const HttpResponse& response, const std::string& operation) {
    if (response.status < 200 || response.status >= 300) {
        throw std::runtime_error(
            operation + " returned HTTP " + std::to_string(response.status) +
            (response.body.empty() ? std::string{} : ": " + response.body));
    }
}

std::string trim_trailing_slashes(std::string value) {
    while (!value.empty() && value.back() == '/') {
        value.pop_back();
    }
    return value;
}

std::string make_guid() {
    static thread_local std::mt19937_64 generator(std::random_device{}());
    static constexpr char hex[] = "0123456789abcdef";
    const int groups[] = {8, 4, 4, 4, 12};
    std::string value;
    for (int group = 0; group < 5; ++group) {
        if (group > 0) {
            value.push_back('-');
        }
        for (int index = 0; index < groups[group]; ++index) {
            value.push_back(hex[generator() & 0x0f]);
        }
    }
    return value;
}

std::string utc_timestamp() {
    const auto now = std::chrono::system_clock::now();
    const auto milliseconds =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            now.time_since_epoch()) %
        1000;
    const std::time_t time = std::chrono::system_clock::to_time_t(now);
    std::tm utc{};
    gmtime_s(&utc, &time);

    std::ostringstream output;
    output << std::put_time(&utc, "%Y-%m-%dT%H:%M:%S") << '.'
           << std::setfill('0') << std::setw(3) << milliseconds.count()
           << 'Z';
    return output.str();
}

std::string url_encode(CURL* curl, const std::string& value) {
    char* encoded = curl_easy_escape(
        curl, value.c_str(), static_cast<int>(value.size()));
    if (encoded == nullptr) {
        throw std::runtime_error("Unable to URL-encode ingestion parameter");
    }
    std::string result(encoded);
    curl_free(encoded);
    return result;
}

std::string get_temp_storage(
    const QueuedIngestConfig& config, const std::string& bearer_token) {
    const nlohmann::json request = {{"csl", ".get ingestion resources"}};
    const std::string request_text = request.dump();
    const HttpResponse response = send_request(
        "POST",
        trim_trailing_slashes(config.ingest_uri) + "/v1/rest/mgmt",
        {
            "Authorization: Bearer " + bearer_token,
            "Content-Type: application/json",
            "Accept: application/json",
        },
        {request_text.begin(), request_text.end()});
    require_success(response, "Ingestion resource discovery");

    const auto document = nlohmann::json::parse(response.body);
    for (const auto& row : document.at("Tables").at(0).at("Rows")) {
        if (row.size() >= 2 && row.at(0) == "TempStorage") {
            return row.at(1).get<std::string>();
        }
    }
    throw std::runtime_error(
        "Kusto did not return a temporary ingestion container");
}

std::string make_blob_url(
    const std::string& container_url, const std::string& blob_name) {
    const auto query = container_url.find('?');
    if (query == std::string::npos) {
        throw std::runtime_error(
            "Temporary ingestion container is missing its SAS token");
    }
    return container_url.substr(0, query) + "/" + blob_name +
           container_url.substr(query);
}

std::string upload_avro(
    const std::string& container_url, const std::vector<char>& avro) {
    const std::string blob_url =
        make_blob_url(container_url, "tickpoc-" + make_guid() + ".avro");
    const HttpResponse response = send_request(
        "PUT",
        blob_url,
        {
            "Content-Type: application/octet-stream",
            "x-ms-blob-type: BlockBlob",
            "x-ms-version: 2023-11-03",
        },
        avro);
    require_success(response, "Avro blob upload");
    return blob_url;
}

}  // namespace

std::string queue_stock_ticks_avro(
    const std::vector<StockTick>& ticks,
    const QueuedIngestConfig& config,
    const std::string& bearer_token) {
    if (config.ingest_uri.empty()) {
        throw std::invalid_argument("ingest_uri must not be empty");
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
    const std::vector<char> avro = serialize_stock_ticks_avro(ticks);
    const std::string blob_url =
        upload_avro(get_temp_storage(config, bearer_token), avro);
    const std::string source_id = make_guid();

    const nlohmann::json request = {
        {"timestamp", utc_timestamp()},
        {"blobs",
         {{{"url", blob_url},
           {"sourceId", source_id},
           {"rawSize", avro.size()}}}},
        {"properties",
         {
             {"format", "avro"},
             {"enableTracking", true},
             {"deleteAfterDownload", true},
             {"ingestionMappingReference", config.mapping},
             {"tags", {"ingest-by:tickpoc"}},
         }},
    };
    const std::string request_text = request.dump();

    CURL* raw_curl = curl_easy_init();
    if (raw_curl == nullptr) {
        throw std::runtime_error("curl_easy_init failed");
    }
    std::unique_ptr<CURL, decltype(&curl_easy_cleanup)> curl(
        raw_curl, &curl_easy_cleanup);
    const std::string url =
        trim_trailing_slashes(config.ingest_uri) +
        "/v1/rest/ingestion/queued/" +
        url_encode(curl.get(), config.database) + "/" +
        url_encode(curl.get(), config.table);

    const HttpResponse response = send_request(
        "POST",
        url,
        {
            "Authorization: Bearer " + bearer_token,
            "Content-Type: application/json",
            "Accept: application/json",
        },
        {request_text.begin(), request_text.end()});
    require_success(response, "Queued ingestion submission");

    const auto document = nlohmann::json::parse(response.body);
    return document.value("ingestionOperationId", source_id);
}

}  // namespace tickpoc
