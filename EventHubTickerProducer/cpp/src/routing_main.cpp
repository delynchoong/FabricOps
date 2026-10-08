#include "environment_config.h"
#include "stock_tick.h"

#include <azure/identity.hpp>
#include <azure/messaging/eventhubs.hpp>

#include <charconv>
#include <chrono>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <memory>
#include <random>
#include <stdexcept>
#include <string>
#include <string_view>

namespace {

struct Options {
    int count = 10;
    int batch_size = 10;
    std::string env_file = ".env";
    bool env_file_explicit = false;
};

int parse_positive_int(const char* value, std::string_view option) {
    const std::string_view input(value);
    int parsed = 0;
    const auto [end, error] =
        std::from_chars(input.data(), input.data() + input.size(), parsed);
    if (error != std::errc{} || end != input.data() + input.size() ||
        parsed <= 0) {
        throw std::invalid_argument(
            std::string(option) + " must be a positive integer");
    }
    return parsed;
}

Options parse_options(int argc, char* argv[]) {
    Options options;
    for (int index = 1; index < argc; ++index) {
        const std::string_view argument(argv[index]);
        if (argument == "--count" && index + 1 < argc) {
            options.count = parse_positive_int(argv[++index], "--count");
        } else if (argument == "--batch-size" && index + 1 < argc) {
            options.batch_size =
                parse_positive_int(argv[++index], "--batch-size");
        } else if (argument == "--env-file" && index + 1 < argc) {
            options.env_file = argv[++index];
            options.env_file_explicit = true;
        } else if (argument == "--help") {
            std::cout
                << "Usage: eventhub_ticker_routing_producer "
                   "[--count EVENTS] [--batch-size EVENTS] "
                   "[--env-file PATH]\n";
            std::exit(0);
        } else {
            throw std::invalid_argument(
                "Unknown or incomplete argument: " + std::string(argument));
        }
    }
    return options;
}

struct Route {
    const char* feed;
    const char* table;
    const char* mapping;
};

constexpr Route kTasRoute{
    "tas",
    "TASTicks",
    "TASTicksAvroMapping",
};

constexpr Route kTaqRoute{
    "taq",
    "TAQTicks",
    "TAQTicksAvroMapping",
};

}  // namespace

int main(int argc, char* argv[]) {
    try {
        const Options options = parse_options(argc, argv);
        tickpoc::load_environment_file(
            options.env_file, options.env_file_explicit);

        Azure::Messaging::EventHubs::ProducerClient producer(
            tickpoc::require_environment_variable("EVENTHUBS_HOST"),
            tickpoc::require_environment_variable("EVENTHUB_NAME"),
            std::make_shared<Azure::Identity::DefaultAzureCredential>());

        std::mt19937_64 random(std::random_device{}());
        std::uniform_real_distribution<double> tas_price(100.0, 200.0);
        std::uniform_real_distribution<double> taq_price(300.0, 400.0);

        auto batch = producer.CreateBatch();
        int batch_event_count = 0;
        int sent_event_count = 0;
        int sent_batch_count = 0;

        const auto send_batch = [&] {
            producer.Send(batch);
            ++sent_batch_count;
            sent_event_count += batch_event_count;
            std::cout << "Sent batch " << sent_batch_count
                      << ": events=" << batch_event_count
                      << ", totalEvents=" << sent_event_count << '\n';
            batch = producer.CreateBatch();
            batch_event_count = 0;
        };

        for (int index = 0; index < options.count; ++index) {
            const Route& route = index % 2 == 0 ? kTasRoute : kTaqRoute;
            const double price =
                route.feed == std::string_view("tas")
                    ? tas_price(random)
                    : taq_price(random);

            auto tick = tickpoc::make_stock_tick(
                route.feed == std::string_view("tas") ? "TAS" : "TAQ",
                price);
            tick.eventname = std::string(route.feed) + " ticks";
            tick.eventdesc =
                std::string(route.feed) + " dynamic routing test";

            Azure::Messaging::EventHubs::Models::EventData event;
            event.Body = tickpoc::serialize_stock_tick_avro(tick);
            event.ContentType = "avro/binary";
            event.MessageId = Azure::Core::Amqp::Models::AmqpValue(
                std::string(route.feed) + "-" +
                std::to_string(tick.eventtime) + "-" +
                std::to_string(index));

            // These case-sensitive properties override the table and mapping
            // configured on the Eventhouse data connection.
            event.Properties["Table"] =
                Azure::Core::Amqp::Models::AmqpValue(route.table);
            event.Properties["Format"] =
                Azure::Core::Amqp::Models::AmqpValue("Avro");
            event.Properties["IngestionMappingReference"] =
                Azure::Core::Amqp::Models::AmqpValue(route.mapping);
            event.Properties["Compression"] =
                Azure::Core::Amqp::Models::AmqpValue("None");
            event.Properties["avro.schema.name"] =
                Azure::Core::Amqp::Models::AmqpValue("tickpoc.StockTick");
            event.Properties["feed"] =
                Azure::Core::Amqp::Models::AmqpValue(route.feed);

            if (!batch.TryAdd(event)) {
                if (batch.NumberOfEvents() == 0) {
                    throw std::runtime_error(
                        "A routed event exceeds the Event Hubs batch size");
                }
                send_batch();
                if (!batch.TryAdd(event)) {
                    throw std::runtime_error(
                        "A routed event exceeds the Event Hubs batch size");
                }
            }

            ++batch_event_count;
            std::cout << "Queued " << route.feed
                      << " event: Table=" << route.table
                      << ", IngestionMappingReference=" << route.mapping
                      << ", ticker=" << tick.ticker
                      << ", price=" << std::fixed << std::setprecision(2)
                      << tick.price << '\n';

            if (batch_event_count == options.batch_size) {
                send_batch();
            }
        }

        if (batch.NumberOfEvents() > 0) {
            send_batch();
        }

        std::cout << "Completed routed publish: eventsSent="
                  << sent_event_count
                  << ", batchesSent=" << sent_batch_count << '\n';
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
