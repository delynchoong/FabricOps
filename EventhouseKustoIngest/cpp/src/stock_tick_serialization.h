#pragma once

#include "stock_tick_ingestion.h"

#include <vector>

namespace tickpoc::detail {

std::vector<char> serialize_stock_ticks_avro(
    const std::vector<StockTick>& ticks);

void validate_ingestion_target(const IngestionTarget& target);

}  // namespace tickpoc::detail
