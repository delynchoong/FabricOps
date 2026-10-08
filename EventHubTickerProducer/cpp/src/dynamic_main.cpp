#include "dynamic_tick.h"
#include "environment_config.h"

#include <azure/identity.hpp>
#include <azure/messaging/eventhubs.hpp>

#include <algorithm>
#include <charconv>
#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <memory>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>

namespace {

struct Options {
    int count = 10;
    int batch_size = 10;
    std::string env_file = ".env";
    bool env_file_explicit = false;
    bool print_message = false;
    std::string dump_avro;
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
        } else if (argument == "--print-message") {
            options.print_message = true;
        } else if (argument == "--dump-avro" && index + 1 < argc) {
            options.dump_avro = argv[++index];
        } else if (argument == "--help") {
            std::cout
                << "Usage: eventhub_ticker_dynamic_producer "
                   "[--count EVENTS] [--batch-size EVENTS] "
                   "[--env-file PATH] [--print-message] "
                   "[--dump-avro PATH]\n";
            std::exit(0);
        } else {
            throw std::invalid_argument(
                "Unknown or incomplete argument: " + std::string(argument));
        }
    }
    if (!options.dump_avro.empty() && options.count != 1) {
        throw std::invalid_argument(
            "--dump-avro requires --count 1 so the file exactly identifies "
            "the transmitted EventData body");
    }
    return options;
}

void append_json_value(
    std::ostream& output,
    const tickpoc::DynamicValue& value) {
    if (std::holds_alternative<std::monostate>(value)) {
        output << "null";
    } else if (const auto* text = std::get_if<std::string>(&value)) {
        output << std::quoted(*text);
    } else {
        output << (std::get<bool>(value) ? "true" : "false");
    }
}

void print_message(
    const tickpoc::DynamicTick& dynamic_tick,
    const std::vector<std::uint8_t>& payload) {
    std::ostringstream record;
    record << "{\"eventname\":" << std::quoted(dynamic_tick.tick.eventname)
           << ",\"eventtime\":" << dynamic_tick.tick.eventtime
           << ",\"ticker\":" << std::quoted(dynamic_tick.tick.ticker)
           << ",\"price\":" << std::setprecision(15)
           << dynamic_tick.tick.price
           << ",\"eventdesc\":" << std::quoted(dynamic_tick.tick.eventdesc)
           << ",\"variablefields\":{";
    for (std::size_t index = 0;
         index < dynamic_tick.variablefields.size();
         ++index) {
        if (index > 0) {
            record << ',';
        }
        record << "\"variablefield" << index + 1 << "\":";
        append_json_value(record, dynamic_tick.variablefields[index]);
    }
    record << "}}";

    std::ostringstream hex;
    hex << std::hex << std::setfill('0');
    const std::size_t preview_size =
        std::min<std::size_t>(payload.size(), 32);
    for (std::size_t index = 0; index < preview_size; ++index) {
        if (index > 0) {
            hex << ' ';
        }
        hex << std::setw(2) << static_cast<unsigned int>(payload[index]);
    }

    std::cout << "Avro logical record: " << record.str() << '\n'
              << "EventData properties: "
                 "Table=DynamicTicks, Format=Avro, "
                 "IngestionMappingReference=DynamicTicksAvroMapping, "
                 "Compression=None\n"
              << "Avro OCF body: bytes=" << payload.size()
              << ", magic=4f 62 6a 01 (Obj\\x01)\n"
              << "EventData.Body hex[0.." << preview_size - 1
              << "]: " << hex.str() << '\n';
}

void dump_and_verify_payload(
    const std::string& path,
    const tickpoc::DynamicTick& expected,
    const std::vector<std::uint8_t>& payload) {
    {
        std::ofstream output(path, std::ios::binary);
        if (!output) {
            throw std::runtime_error(
                "Unable to create Avro dump file: " + path);
        }
        output.write(
            reinterpret_cast<const char*>(payload.data()),
            static_cast<std::streamsize>(payload.size()));
    }

    std::ifstream input(path, std::ios::binary);
    if (!input) {
        throw std::runtime_error("Unable to reopen Avro dump file: " + path);
    }
    const std::vector<std::uint8_t> dumped{
        std::istreambuf_iterator<char>(input),
        std::istreambuf_iterator<char>()};
    if (dumped != payload) {
        throw std::runtime_error(
            "Avro dump bytes differ from the EventData body");
    }

    const auto decoded = tickpoc::deserialize_dynamic_tick_avro(dumped);
    if (decoded.tick.eventname != expected.tick.eventname ||
        decoded.tick.eventtime != expected.tick.eventtime ||
        decoded.tick.ticker != expected.tick.ticker ||
        decoded.tick.price != expected.tick.price ||
        decoded.tick.eventdesc != expected.tick.eventdesc ||
        decoded.variablefields != expected.variablefields) {
        throw std::runtime_error(
            "Avro dump did not decode to the transmitted logical record");
    }

    std::cout << "Wrote exact EventData.Body to " << path
              << " and decoded it successfully as Avro OCF\n";
}

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
        std::uniform_real_distribution<double> price(100.0, 500.0);

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
            auto stock_tick =
                tickpoc::make_stock_tick("DYN" + std::to_string(index + 1),
                                         price(random));
            stock_tick.eventname = "dynamic stock ticks";
            stock_tick.eventdesc =
                "nested Avro record to KQL dynamic column";
            const auto dynamic_tick =
                tickpoc::make_dynamic_tick(std::move(stock_tick), index);
            const auto payload =
                tickpoc::serialize_dynamic_tick_avro(dynamic_tick);

            if (options.print_message) {
                print_message(dynamic_tick, payload);
            }
            if (!options.dump_avro.empty()) {
                dump_and_verify_payload(
                    options.dump_avro, dynamic_tick, payload);
            }

            Azure::Messaging::EventHubs::Models::EventData event;
            event.Body = payload;
            event.ContentType = "avro/binary";
            event.MessageId = Azure::Core::Amqp::Models::AmqpValue(
                "dynamic-" + std::to_string(dynamic_tick.tick.eventtime) +
                "-" + std::to_string(index));
            event.Properties["Table"] =
                Azure::Core::Amqp::Models::AmqpValue("DynamicTicks");
            event.Properties["Format"] =
                Azure::Core::Amqp::Models::AmqpValue("Avro");
            event.Properties["IngestionMappingReference"] =
                Azure::Core::Amqp::Models::AmqpValue(
                    "DynamicTicksAvroMapping");
            event.Properties["Compression"] =
                Azure::Core::Amqp::Models::AmqpValue("None");
            event.Properties["avro.schema.name"] =
                Azure::Core::Amqp::Models::AmqpValue(
                    "tickpoc.DynamicTick");

            if (!batch.TryAdd(event)) {
                if (batch.NumberOfEvents() == 0) {
                    throw std::runtime_error(
                        "A dynamic event exceeds the Event Hubs batch size");
                }
                send_batch();
                if (!batch.TryAdd(event)) {
                    throw std::runtime_error(
                        "A dynamic event exceeds the Event Hubs batch size");
                }
            }

            ++batch_event_count;
            std::cout << "Queued dynamic event: ticker="
                      << dynamic_tick.tick.ticker
                      << ", populatedFields=" << index % 10 + 1
                      << ", price=" << std::fixed << std::setprecision(2)
                      << dynamic_tick.tick.price << '\n';

            if (batch_event_count == options.batch_size) {
                send_batch();
            }
        }

        if (batch.NumberOfEvents() > 0) {
            send_batch();
        }

        std::cout << "Completed dynamic publish: eventsSent="
                  << sent_event_count
                  << ", batchesSent=" << sent_batch_count << '\n';
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
