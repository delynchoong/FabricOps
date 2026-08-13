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

struct IngestConfig {
    std::string streaming_ingest_uri;
    std::string database;
    std::string table;
    std::string mapping;
};

StockTick make_stock_tick(
    std::string ticker,
    double price,
    std::chrono::system_clock::time_point event_time =
        std::chrono::system_clock::now());

void ingest_stock_tick_avro(
    const StockTick& tick,
    const IngestConfig& config,
    const std::string& bearer_token);

std::vector<char> serialize_stock_ticks_avro(
    const std::vector<StockTick>& ticks);

void ingest_stock_ticks_avro(
    const std::vector<StockTick>& ticks,
    const IngestConfig& config,
    const std::string& bearer_token);

}  // namespace tickpoc
