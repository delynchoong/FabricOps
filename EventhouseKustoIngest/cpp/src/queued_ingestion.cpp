#include "stock_tick_ingestion.h"

#include "http_client.h"
#include "stock_tick_serialization.h"

#include <nlohmann/json.hpp>

#include <chrono>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace tickpoc {
namespace {

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

std::string get_temp_storage(
    const QueuedIngestConfig& config, const std::string& bearer_token) {
    const nlohmann::json request = {
        {"db", config.target.database},
        {"csl", ".get ingestion resources"},
    };
    const std::string request_text = request.dump();
    const auto response = detail::send_http_request(
        "POST",
        detail::trim_trailing_slashes(config.ingestion_uri) + "/v1/rest/mgmt",
        {
            "Authorization: Bearer " + bearer_token,
            "Content-Type: application/json",
            "Accept: application/json",
            "x-ms-app: EventhouseKustoIngest",
            "x-ms-client-version: 1.0.0",
            "x-ms-client-request-id: EventhouseKustoIngest.Resources;" +
                detail::make_guid(),
        },
        {request_text.begin(), request_text.end()},
        60L);
    detail::require_http_success(response, "Ingestion resource discovery");

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
        make_blob_url(
            container_url,
            "eventhouse-kusto-ingest-" + detail::make_guid() + ".avro");
    const auto response = detail::send_http_request(
        "PUT",
        blob_url,
        {
            "Content-Type: application/octet-stream",
            "x-ms-blob-type: BlockBlob",
            "x-ms-version: 2023-11-03",
        },
        avro,
        60L);
    detail::require_http_success(response, "Avro blob upload");
    return blob_url;
}

}  // namespace

std::string ingest_stock_ticks_queued(
    const std::vector<StockTick>& ticks,
    const QueuedIngestConfig& config,
    const std::string& bearer_token) {
    if (config.ingestion_uri.empty()) {
        throw std::invalid_argument("ingestion_uri must not be empty");
    }
    detail::validate_ingestion_target(config.target);
    if (bearer_token.empty()) {
        throw std::invalid_argument("bearer_token must not be empty");
    }

    const std::vector<char> avro =
        detail::serialize_stock_ticks_avro(ticks);
    const std::string blob_url =
        upload_avro(get_temp_storage(config, bearer_token), avro);
    const std::string source_id = detail::make_guid();

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
             {"ingestionMappingReference", config.target.mapping},
             {"tags",
              nlohmann::json::array(
                  {"ingest-by:eventhouse-kusto-ingest"})},
         }},
    };
    const std::string request_text = request.dump();

    const std::string url =
        detail::trim_trailing_slashes(config.ingestion_uri) +
        "/v1/rest/ingestion/queued/" +
        detail::url_encode(config.target.database) + "/" +
        detail::url_encode(config.target.table);

    const auto response = detail::send_http_request(
        "POST",
        url,
        {
            "Authorization: Bearer " + bearer_token,
            "Content-Type: application/json",
            "Accept: application/json",
            "x-ms-app: EventhouseKustoIngest",
            "x-ms-client-version: 1.0.0",
            "x-ms-client-request-id: EventhouseKustoIngest.Queued;" +
                detail::make_guid(),
        },
        {request_text.begin(), request_text.end()},
        60L);
    detail::require_http_success(response, "Queued ingestion submission");

    const auto document = nlohmann::json::parse(response.body);
    const std::string operation_id =
        document.value("ingestionOperationId", std::string{});
    if (operation_id.empty()) {
        throw std::runtime_error(
            "Queued ingestion tracking was enabled, but the service did not "
            "return an ingestionOperationId");
    }
    return operation_id;
}

}  // namespace tickpoc
