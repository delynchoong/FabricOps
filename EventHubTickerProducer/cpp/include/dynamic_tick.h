#pragma once

#include "stock_tick.h"

#include <array>
#include <cstdint>
#include <string>
#include <variant>
#include <vector>

namespace tickpoc {

using DynamicValue = std::variant<std::monostate, std::string, bool>;

struct DynamicTick {
    StockTick tick;
    std::array<DynamicValue, 10> variablefields;
};

DynamicTick make_dynamic_tick(StockTick tick, int pattern);
std::vector<std::uint8_t> serialize_dynamic_tick_avro(
    const DynamicTick& tick);
DynamicTick deserialize_dynamic_tick_avro(
    const std::vector<std::uint8_t>& payload);

}  // namespace tickpoc
