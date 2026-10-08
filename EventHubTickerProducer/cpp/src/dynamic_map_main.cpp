#include "dynamic_map_tick.h"
#include "environment_config.h"

#include <azure/identity.hpp>
#include <azure/messaging/eventhubs.hpp>

#include <algorithm>
#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>

// Recommended Fabric Eventhouse path validated by this PoC: one complete Avro
// OCF contains a fixed top-level record plus a variablefields map. The fixed
// field labels live in the writer schema; arbitrary map keys remain in each
// map entry and land in the KQL dynamic column.
namespace {

struct Options {
    std::string env_file = ".env";
    bool env_file_explicit = false;
    bool print_message = false;
    std::string dump_avro;
};

Options parse_options(int argc, char* argv[]) {
    Options options;
    for (int index = 1; index < argc; ++index) {
        const std::string_view argument(argv[index]);
        if (argument == "--env-file" && index + 1 < argc) {
            options.env_file = argv[++index];
            options.env_file_explicit = true;
        } else if (argument == "--print-message") {
            options.print_message = true;
        } else if (argument == "--dump-avro" && index + 1 < argc) {
            options.dump_avro = argv[++index];
        } else if (argument == "--help") {
            std::cout
                << "Usage: eventhub_ticker_dynamic_map_producer "
                   "[--env-file PATH] [--print-message] "
                   "[--dump-avro PATH]\n";
            std::exit(0);
        } else {
            throw std::invalid_argument(
                "Unknown or incomplete argument: " + std::string(argument));
        }
    }
    return options;
}

void append_json_value(
    std::ostream& output,
    const tickpoc::MapValue& value) {
    if (std::holds_alternative<std::monostate>(value)) {
        output << "null";
    } else if (const auto* text = std::get_if<std::string>(&value)) {
        output << std::quoted(*text);
    } else if (const auto* flag = std::get_if<bool>(&value)) {
        output << (*flag ? "true" : "false");
    } else if (const auto* number = std::get_if<std::int64_t>(&value)) {
        output << *number;
    } else {
        output << std::setprecision(15) << std::get<double>(value);
    }
}

void print_message(
    const std::vector<tickpoc::DynamicMapTick>& ticks,
    const std::vector<std::uint8_t>& payload) {
    for (std::size_t record_index = 0;
         record_index < ticks.size();
         ++record_index) {
        const auto& tick = ticks[record_index];
        std::ostringstream record;
        record << "{\"eventname\":" << std::quoted(tick.tick.eventname)
               << ",\"eventtime\":" << tick.tick.eventtime
               << ",\"ticker\":" << std::quoted(tick.tick.ticker)
               << ",\"price\":" << std::setprecision(15)
               << tick.tick.price
               << ",\"eventdesc\":" << std::quoted(tick.tick.eventdesc)
               << ",\"variablefields\":{";
        std::size_t field_index = 0;
        for (const auto& [key, value] : tick.variablefields) {
            if (field_index++ > 0) {
                record << ',';
            }
            record << std::quoted(key) << ':';
            append_json_value(record, value);
        }
        record << "}}";
        std::cout << "Avro map record " << record_index + 1
                  << ": " << record.str() << '\n';
    }

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

    std::cout << "EventData properties: "
                 "Table=DynamicTicks, Format=Avro, "
                 "IngestionMappingReference=DynamicTicksAvroMapping, "
                 "Compression=None\n"
              << "Avro OCF body: records=" << ticks.size()
              << ", dataBlocks="
              << tickpoc::count_avro_ocf_data_blocks(payload)
              << ", bytes=" << payload.size()
              << ", magic=4f 62 6a 01 (Obj\\x01)\n"
              << "EventData.Body hex[0.." << preview_size - 1
              << "]: " << hex.str() << '\n';
}

void dump_and_verify_payload(
    const std::string& path,
    const std::vector<tickpoc::DynamicMapTick>& expected,
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

    const auto decoded =
        tickpoc::deserialize_dynamic_map_ticks_avro(dumped);
    if (decoded.size() != expected.size()) {
        throw std::runtime_error(
            "Avro map dump record count changed during decoding");
    }
    for (std::size_t index = 0; index < expected.size(); ++index) {
        if (decoded[index].tick.eventname !=
                expected[index].tick.eventname ||
            decoded[index].tick.eventtime !=
                expected[index].tick.eventtime ||
            decoded[index].tick.ticker != expected[index].tick.ticker ||
            decoded[index].tick.price != expected[index].tick.price ||
            decoded[index].tick.eventdesc !=
                expected[index].tick.eventdesc ||
            decoded[index].variablefields !=
                expected[index].variablefields) {
            throw std::runtime_error(
                "Avro map dump did not decode to the transmitted records");
        }
    }
    if (tickpoc::count_avro_ocf_data_blocks(dumped) != expected.size()) {
        throw std::runtime_error(
            "Avro map dump does not contain one data block per record");
    }

    std::cout << "Wrote exact five-block EventData.Body to " << path
              << " and decoded all records successfully as Avro OCF\n";
}

}  // namespace

int main(int argc, char* argv[]) {
    try {
        const Options options = parse_options(argc, argv);
        tickpoc::load_environment_file(
            options.env_file, options.env_file_explicit);

        const auto ticks = tickpoc::make_dynamic_map_ticks();
        const auto payload =
            tickpoc::serialize_dynamic_map_ticks_avro(ticks);
        if (tickpoc::count_avro_ocf_data_blocks(payload) != ticks.size()) {
            throw std::runtime_error(
                "Expected five Avro data blocks in one OCF message");
        }

        if (options.print_message) {
            print_message(ticks, payload);
        }
        if (!options.dump_avro.empty()) {
            dump_and_verify_payload(options.dump_avro, ticks, payload);
        }

        Azure::Messaging::EventHubs::ProducerClient producer(
            tickpoc::require_environment_variable("EVENTHUBS_HOST"),
            tickpoc::require_environment_variable("EVENTHUB_NAME"),
            std::make_shared<Azure::Identity::DefaultAzureCredential>());

        Azure::Messaging::EventHubs::Models::EventData event;
        event.Body = payload;
        event.ContentType = "avro/binary";
        event.MessageId = Azure::Core::Amqp::Models::AmqpValue(
            "dynamic-map-" + std::to_string(ticks.front().tick.eventtime));
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
                "tickpoc.DynamicMapTick");

        auto batch = producer.CreateBatch();
        if (!batch.TryAdd(event)) {
            throw std::runtime_error(
                "The five-block Avro map event exceeds the batch size");
        }
        producer.Send(batch);

        std::cout << "Sent one EventData message containing "
                  << ticks.size() << " Avro records in "
                  << tickpoc::count_avro_ocf_data_blocks(payload)
                  << " data blocks\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
