#include "ticker_simulation.h"

#include "environment_config.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <cmath>
#include <csignal>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <random>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace {

struct TickerState {
    std::string symbol;
    double base_price;
    double price;
};

struct Options {
    std::string env_file = ".env";
    bool env_file_explicit = false;
    int rate = 100;
    int duration_seconds = 0;
};

volatile std::sig_atomic_t stop_requested = 0;

void handle_signal(int) {
    stop_requested = 1;
}

int parse_positive_int(const std::string& value, const std::string& option) {
    const int parsed = std::stoi(value);
    if (parsed <= 0) {
        throw std::invalid_argument(option + " must be greater than zero");
    }
    return parsed;
}

Options parse_options(int argc, char* argv[], const char* executable_name) {
    Options options;
    for (int index = 1; index < argc; ++index) {
        const std::string_view argument(argv[index]);
        if (argument == "--env-file" && index + 1 < argc) {
            options.env_file = argv[++index];
            options.env_file_explicit = true;
        } else if (argument == "--rate" && index + 1 < argc) {
            options.rate = parse_positive_int(argv[++index], "--rate");
        } else if (argument == "--duration" && index + 1 < argc) {
            options.duration_seconds =
                parse_positive_int(argv[++index], "--duration");
        } else if (argument == "--help") {
            std::cout << "Usage: " << executable_name
                      << " [--rate EVENTS_PER_SECOND] [--duration SECONDS] "
                         "[--env-file PATH]\n";
            std::exit(0);
        } else {
            throw std::invalid_argument(
                "Unknown or incomplete argument: " + std::string(argument));
        }
    }
    return options;
}

std::vector<tickpoc::StockTick> generate_batch(
    std::array<TickerState, 5>& tickers,
    int count,
    std::mt19937_64& random) {
    std::uniform_int_distribution<std::size_t> choose_ticker(
        0, tickers.size() - 1);
    std::uniform_real_distribution<double> move(-0.002, 0.002);

    std::vector<tickpoc::StockTick> events;
    events.reserve(static_cast<std::size_t>(count));
    for (int index = 0; index < count; ++index) {
        auto& ticker = tickers[choose_ticker(random)];
        ticker.price *= 1.0 + move(random);
        ticker.price = std::clamp(
            ticker.price, ticker.base_price * 0.90, ticker.base_price * 1.10);
        ticker.price = std::round(ticker.price * 100.0) / 100.0;
        events.push_back(
            tickpoc::make_stock_tick(ticker.symbol, ticker.price));
    }
    return events;
}

}  // namespace

namespace tickpoc {

int run_ticker_simulation(
    int argc,
    char* argv[],
    AvroPayloadFormat payload_format,
    const char* executable_name) {
    try {
        const Options options = parse_options(argc, argv, executable_name);
        load_environment_file(options.env_file, options.env_file_explicit);

        EventHubProducer producer(
            require_environment_variable("EVENTHUBS_HOST"),
            require_environment_variable("EVENTHUB_NAME"),
            payload_format);

        std::array<TickerState, 5> tickers = {{
            {"AAPL", 230.00, 230.00},
            {"MSFT", 420.00, 420.00},
            {"NVDA", 180.00, 180.00},
            {"AMZN", 225.00, 225.00},
            {"GOOGL", 200.00, 200.00},
        }};
        std::mt19937_64 random(std::random_device{}());
        std::signal(SIGINT, handle_signal);

        const auto started = std::chrono::steady_clock::now();
        auto next_batch = started;
        std::uint64_t total = 0;
        while (!stop_requested) {
            if (options.duration_seconds > 0 &&
                std::chrono::steady_clock::now() - started >=
                    std::chrono::seconds(options.duration_seconds)) {
                break;
            }

            next_batch += std::chrono::seconds(1);
            auto events = generate_batch(tickers, options.rate, random);
            producer.publish(events);
            total += events.size();
            std::cout << "sent " << events.size() << " events; total=" << total
                      << '\n';
            if (std::chrono::steady_clock::now() > next_batch) {
                next_batch = std::chrono::steady_clock::now();
            }
            std::this_thread::sleep_until(next_batch);
        }

        std::cout << "Stopped after sending " << total << " events\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}

}  // namespace tickpoc
