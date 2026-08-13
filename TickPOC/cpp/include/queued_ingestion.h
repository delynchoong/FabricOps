#pragma once

#include "stock_tick_producer.h"

#include <string>
#include <vector>

namespace tickpoc {

struct QueuedIngestConfig {
    std::string ingest_uri;
    std::string database;
    std::string table;
    std::string mapping;
};

std::string queue_stock_ticks_avro(
    const std::vector<StockTick>& ticks,
    const QueuedIngestConfig& config,
    const std::string& bearer_token);

}  // namespace tickpoc
