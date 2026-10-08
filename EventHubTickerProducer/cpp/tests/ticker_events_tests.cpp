#include "dynamic_map_tick.h"
#include "dynamic_tick.h"
#include "stock_tick.h"

#include <chrono>
#include <cmath>
#include <functional>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

void require(bool condition, const std::string& message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

void require_invalid_argument(
    const std::function<void()>& action,
    const std::string& message) {
    try {
        action();
    } catch (const std::invalid_argument&) {
        return;
    }
    throw std::runtime_error(message);
}

}  // namespace

int main() {
    try {
        const auto event_time =
            std::chrono::system_clock::time_point(std::chrono::milliseconds(
                1'700'000'000'123));
        const auto expected =
            tickpoc::make_stock_tick("MSFT", 420.25, event_time);

        const auto payload = tickpoc::serialize_stock_tick_avro(expected);
        require(payload.size() > 4, "Avro payload is empty");
        require(
            payload[0] == 'O' && payload[1] == 'b' && payload[2] == 'j' &&
                payload[3] == 1,
            "Payload is not an Avro object container file");

        const auto actual = tickpoc::deserialize_stock_tick_avro(payload);

        require(actual.eventname == expected.eventname, "Event name changed");
        require(actual.eventtime == expected.eventtime, "Event time changed");
        require(actual.ticker == expected.ticker, "Ticker changed");
        require(actual.price == expected.price, "Price changed");
        require(actual.eventdesc == expected.eventdesc, "Description changed");

        const auto raw_payload =
            tickpoc::serialize_stock_tick_avro_raw(expected);
        require(!raw_payload.empty(), "Raw Avro payload is empty");
        require(
            raw_payload.size() < 4 ||
                raw_payload[0] != 'O' || raw_payload[1] != 'b' ||
                raw_payload[2] != 'j' || raw_payload[3] != 1,
            "Raw Avro payload contains an object container header");

        const auto raw_actual =
            tickpoc::deserialize_stock_tick_avro_raw(raw_payload);
        require(
            raw_actual.eventname == expected.eventname,
            "Raw Avro event name changed");
        require(
            raw_actual.eventtime == expected.eventtime,
            "Raw Avro event time changed");
        require(
            raw_actual.ticker == expected.ticker,
            "Raw Avro ticker changed");
        require(
            raw_actual.price == expected.price,
            "Raw Avro price changed");
        require(
            raw_actual.eventdesc == expected.eventdesc,
            "Raw Avro description changed");

        for (const int pattern : {0, 4, 9}) {
            const auto expected_dynamic =
                tickpoc::make_dynamic_tick(expected, pattern);
            const auto dynamic_payload =
                tickpoc::serialize_dynamic_tick_avro(expected_dynamic);
            require(
                dynamic_payload.size() > 4,
                "Dynamic Avro payload is empty");
            require(
                dynamic_payload[0] == 'O' &&
                    dynamic_payload[1] == 'b' &&
                    dynamic_payload[2] == 'j' &&
                    dynamic_payload[3] == 1,
                "Dynamic payload is not an Avro object container file");

            const auto actual_dynamic =
                tickpoc::deserialize_dynamic_tick_avro(dynamic_payload);
            require(
                actual_dynamic.tick.eventname ==
                    expected_dynamic.tick.eventname,
                "Dynamic Avro event name changed");
            require(
                actual_dynamic.tick.eventtime ==
                    expected_dynamic.tick.eventtime,
                "Dynamic Avro event time changed");
            require(
                actual_dynamic.tick.ticker == expected_dynamic.tick.ticker,
                "Dynamic Avro ticker changed");
            require(
                actual_dynamic.tick.price == expected_dynamic.tick.price,
                "Dynamic Avro price changed");
            require(
                actual_dynamic.tick.eventdesc ==
                    expected_dynamic.tick.eventdesc,
                "Dynamic Avro description changed");
            require(
                actual_dynamic.variablefields ==
                    expected_dynamic.variablefields,
                "Dynamic Avro nested fields changed");
        }

        const auto expected_map_ticks =
            tickpoc::make_dynamic_map_ticks();
        const auto map_payload =
            tickpoc::serialize_dynamic_map_ticks_avro(expected_map_ticks);
        require(
            map_payload.size() > 4,
            "Dynamic map Avro payload is empty");
        require(
            map_payload[0] == 'O' && map_payload[1] == 'b' &&
                map_payload[2] == 'j' && map_payload[3] == 1,
            "Dynamic map payload is not an Avro object container file");
        require(
            tickpoc::count_avro_ocf_data_blocks(map_payload) == 5,
            "Dynamic map payload does not contain five data blocks");
        auto map_payload_with_invalid_sync = map_payload;
        map_payload_with_invalid_sync.back() ^= 0xffU;
        require_invalid_argument(
            [&map_payload_with_invalid_sync] {
                tickpoc::count_avro_ocf_data_blocks(
                    map_payload_with_invalid_sync);
            },
            "Dynamic map payload with an invalid sync marker was accepted");

        const auto actual_map_ticks =
            tickpoc::deserialize_dynamic_map_ticks_avro(map_payload);
        require(
            actual_map_ticks.size() == expected_map_ticks.size(),
            "Dynamic map Avro record count changed");
        for (std::size_t index = 0;
             index < expected_map_ticks.size();
             ++index) {
            require(
                actual_map_ticks[index].tick.eventname ==
                    expected_map_ticks[index].tick.eventname,
                "Dynamic map event name changed");
            require(
                actual_map_ticks[index].tick.eventtime ==
                    expected_map_ticks[index].tick.eventtime,
                "Dynamic map event time changed");
            require(
                actual_map_ticks[index].tick.ticker ==
                    expected_map_ticks[index].tick.ticker,
                "Dynamic map ticker changed");
            require(
                actual_map_ticks[index].tick.price ==
                    expected_map_ticks[index].tick.price,
                "Dynamic map price changed");
            require(
                actual_map_ticks[index].tick.eventdesc ==
                    expected_map_ticks[index].tick.eventdesc,
                "Dynamic map description changed");
            require(
                actual_map_ticks[index].variablefields ==
                    expected_map_ticks[index].variablefields,
                "Dynamic map values changed");
            require(
                actual_map_ticks[index].variablefields.size() == index + 1,
                "Dynamic map key count changed");
        }

        require_invalid_argument(
            [] { tickpoc::make_stock_tick("", 1.0); },
            "Empty ticker was accepted");
        require_invalid_argument(
            [] { tickpoc::make_stock_tick("MSFT", -1.0); },
            "Negative price was accepted");
        require_invalid_argument(
            [] { tickpoc::deserialize_stock_tick_avro({}); },
            "Empty Avro payload was accepted");
        require_invalid_argument(
            [] { tickpoc::deserialize_stock_tick_avro_raw({}); },
            "Empty raw Avro payload was accepted");
        require_invalid_argument(
            [] {
                tickpoc::make_dynamic_tick(
                    tickpoc::make_stock_tick("MSFT", 1.0), -1);
            },
            "Negative dynamic pattern was accepted");
        require_invalid_argument(
            [] { tickpoc::deserialize_dynamic_tick_avro({}); },
            "Empty dynamic Avro payload was accepted");
        require_invalid_argument(
            [] { tickpoc::serialize_dynamic_map_ticks_avro({}); },
            "Empty dynamic map tick list was accepted");
        require_invalid_argument(
            [] { tickpoc::deserialize_dynamic_map_ticks_avro({}); },
            "Empty dynamic map Avro payload was accepted");
        require_invalid_argument(
            [] { tickpoc::count_avro_ocf_data_blocks({}); },
            "Empty Avro OCF was accepted by the block counter");
        require_invalid_argument(
            [] {
                tickpoc::make_stock_tick(
                    "MSFT", std::numeric_limits<double>::quiet_NaN());
            },
            "NaN price was accepted");
        require_invalid_argument(
            [] { tickpoc::EventHubProducer("", "stock-ticks"); },
            "Empty namespace was accepted");
        require_invalid_argument(
            [] {
                tickpoc::EventHubProducer(
                    "tickpoc.servicebus.windows.net", "");
            },
            "Empty Event Hub name was accepted");

        std::cout << "All ticker event tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
