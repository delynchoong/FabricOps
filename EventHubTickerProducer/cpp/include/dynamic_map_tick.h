#pragma once

#include "stock_tick.h"

#include <cstddef>
#include <cstdint>
#include <map>
#include <string>
#include <variant>
#include <vector>

namespace tickpoc {

using MapValue =
    std::variant<std::monostate, std::string, bool, std::int64_t, double>;

struct DynamicMapTick {
    StockTick tick;
    std::map<std::string, MapValue> variablefields;
};

std::vector<DynamicMapTick> make_dynamic_map_ticks();
std::vector<std::uint8_t> serialize_dynamic_map_ticks_avro(
    const std::vector<DynamicMapTick>& ticks);
std::vector<DynamicMapTick> deserialize_dynamic_map_ticks_avro(
    const std::vector<std::uint8_t>& payload);
std::size_t count_avro_ocf_data_blocks(
    const std::vector<std::uint8_t>& payload);

}  // namespace tickpoc
