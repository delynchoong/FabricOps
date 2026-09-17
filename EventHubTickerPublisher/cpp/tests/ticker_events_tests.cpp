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
                tickpoc::make_stock_tick(
                    "MSFT", std::numeric_limits<double>::quiet_NaN());
            },
            "NaN price was accepted");
        require_invalid_argument(
            [] { tickpoc::EventHubPublisher("", "stock-ticks"); },
            "Empty namespace was accepted");
        require_invalid_argument(
            [] {
                tickpoc::EventHubPublisher(
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
