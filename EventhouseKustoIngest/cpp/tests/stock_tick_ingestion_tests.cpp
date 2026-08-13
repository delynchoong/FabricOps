#include "stock_tick_ingestion.h"

#include "stock_tick_serialization.h"

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
        const auto tick =
            tickpoc::make_stock_tick("MSFT", 420.25, event_time);

        require(tick.eventname == "stock ticks", "Unexpected event name");
        require(tick.eventtime == 1'700'000'000'123, "Incorrect event time");
        require(tick.ticker == "MSFT", "Incorrect ticker");
        require(tick.price == 420.25, "Incorrect price");
        require(
            tick.eventdesc == "stock ticker price",
            "Unexpected event description");

        require_invalid_argument(
            [] { tickpoc::make_stock_tick("", 1.0); },
            "Empty ticker was accepted");
        require_invalid_argument(
            [] { tickpoc::make_stock_tick("MSFT", -1.0); },
            "Negative price was accepted");
        require_invalid_argument(
            [] {
                tickpoc::make_stock_tick(
                    "MSFT", std::numeric_limits<double>::quiet_NaN());
            },
            "NaN price was accepted");

        const std::vector<char> avro =
            tickpoc::detail::serialize_stock_ticks_avro({tick});
        require(avro.size() > 4, "Avro payload is empty");
        require(
            avro[0] == 'O' && avro[1] == 'b' && avro[2] == 'j' &&
                avro[3] == 1,
            "Payload is not an Avro object container file");

        const tickpoc::IngestionTarget invalid_target{};
        require_invalid_argument(
            [&] { tickpoc::detail::validate_ingestion_target(invalid_target); },
            "Empty ingestion target was accepted");

        std::cout << "All stock tick ingestion tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
