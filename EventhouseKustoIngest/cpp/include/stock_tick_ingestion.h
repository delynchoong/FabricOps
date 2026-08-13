#pragma once

#include <chrono>
#include <cstdint>
#include <string>
#include <vector>

namespace tickpoc {

struct StockTick {
    std::string eventname;
    std::int64_t eventtime;
    std::string ticker;
    double price;
    std::string eventdesc;
};

struct IngestionTarget {
    std::string database;
    std::string table;
    std::string mapping;
};

struct StreamingIngestConfig {
    // Kusto hosts the streaming REST endpoint on the query service URI.
    std::string query_uri;
    IngestionTarget target;
};

struct QueuedIngestConfig {
    std::string ingestion_uri;
    IngestionTarget target;
};

StockTick make_stock_tick(
    std::string ticker,
    double price,
    std::chrono::system_clock::time_point event_time =
        std::chrono::system_clock::now());

void ingest_stock_tick_streaming(
    const StockTick& tick,
    const StreamingIngestConfig& config,
    const std::string& bearer_token);

void ingest_stock_ticks_streaming(
    const std::vector<StockTick>& ticks,
    const StreamingIngestConfig& config,
    const std::string& bearer_token);

std::string ingest_stock_ticks_queued(
    const std::vector<StockTick>& ticks,
    const QueuedIngestConfig& config,
    const std::string& bearer_token);

}  // namespace tickpoc
