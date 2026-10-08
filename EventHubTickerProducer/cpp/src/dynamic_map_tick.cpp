#include "dynamic_map_tick.h"

// Avro C++ 1.12.1 uses fmt::format without including fmt/format.h.
#include <fmt/format.h>
#include <avro/Compiler.hh>
#include <avro/DataFile.hh>
#include <avro/Decoder.hh>
#include <avro/Encoder.hh>
#include <avro/Specific.hh>
#include <avro/Stream.hh>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

namespace {

void encode_map_value(
    avro::Encoder& encoder,
    const tickpoc::MapValue& value) {
    if (std::holds_alternative<std::monostate>(value)) {
        encoder.encodeUnionIndex(0);
        encoder.encodeNull();
    } else if (const auto* text = std::get_if<std::string>(&value)) {
        encoder.encodeUnionIndex(1);
        avro::encode(encoder, *text);
    } else if (const auto* flag = std::get_if<bool>(&value)) {
        encoder.encodeUnionIndex(2);
        avro::encode(encoder, *flag);
    } else if (const auto* number = std::get_if<std::int64_t>(&value)) {
        encoder.encodeUnionIndex(3);
        avro::encode(encoder, *number);
    } else {
        encoder.encodeUnionIndex(4);
        avro::encode(encoder, std::get<double>(value));
    }
}

tickpoc::MapValue decode_map_value(avro::Decoder& decoder) {
    switch (decoder.decodeUnionIndex()) {
        case 0:
            decoder.decodeNull();
            return std::monostate{};
        case 1: {
            std::string value;
            avro::decode(decoder, value);
            return value;
        }
        case 2: {
            bool value = false;
            avro::decode(decoder, value);
            return value;
        }
        case 3: {
            std::int64_t value = 0;
            avro::decode(decoder, value);
            return value;
        }
        case 4: {
            double value = 0.0;
            avro::decode(decoder, value);
            return value;
        }
        default:
            throw std::runtime_error(
                "Unexpected Avro union branch for a map value");
    }
}

}  // namespace

namespace avro {

template <>
struct codec_traits<tickpoc::DynamicMapTick> {
    static void encode(
        Encoder& encoder,
        const tickpoc::DynamicMapTick& value) {
        avro::encode(encoder, value.tick.eventname);
        avro::encode(encoder, value.tick.eventtime);
        avro::encode(encoder, value.tick.ticker);
        avro::encode(encoder, value.tick.price);
        avro::encode(encoder, value.tick.eventdesc);

        encoder.mapStart();
        if (!value.variablefields.empty()) {
            encoder.setItemCount(value.variablefields.size());
            for (const auto& [key, map_value] : value.variablefields) {
                encoder.startItem();
                avro::encode(encoder, key);
                encode_map_value(encoder, map_value);
            }
        }
        encoder.mapEnd();
    }

    static void decode(Decoder& decoder, tickpoc::DynamicMapTick& value) {
        avro::decode(decoder, value.tick.eventname);
        avro::decode(decoder, value.tick.eventtime);
        avro::decode(decoder, value.tick.ticker);
        avro::decode(decoder, value.tick.price);
        avro::decode(decoder, value.tick.eventdesc);

        value.variablefields.clear();
        for (std::size_t count = decoder.mapStart();
             count != 0;
             count = decoder.mapNext()) {
            for (std::size_t index = 0; index < count; ++index) {
                std::string key;
                avro::decode(decoder, key);
                value.variablefields.emplace(
                    std::move(key), decode_map_value(decoder));
            }
        }
    }
};

}  // namespace avro

namespace tickpoc {
namespace {

constexpr const char* kDynamicMapAvroSchema = R"({
  "type": "record",
  "name": "DynamicMapTick",
  "namespace": "tickpoc",
  "fields": [
    {"name": "eventname", "type": "string"},
    {"name": "eventtime", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "ticker", "type": "string"},
    {"name": "price", "type": "double"},
    {"name": "eventdesc", "type": "string"},
    {
      "name": "variablefields",
      "type": {
        "type": "map",
        "values": ["null", "string", "boolean", "long", "double"]
      },
      "default": {}
    }
  ]
})";

avro::ValidSchema compile_dynamic_map_schema() {
    std::istringstream schema_stream(kDynamicMapAvroSchema);
    avro::ValidSchema schema;
    avro::compileJsonSchema(schema_stream, schema);
    return schema;
}

std::int64_t decode_avro_long(
    const std::vector<std::uint8_t>& payload,
    std::size_t& offset) {
    std::uint64_t encoded = 0;
    for (unsigned int byte_index = 0; byte_index < 10; ++byte_index) {
        if (offset >= payload.size()) {
            throw std::invalid_argument(
                "Avro OCF ended while decoding a long");
        }
        const std::uint8_t byte = payload[offset++];
        if (byte_index == 9 && (byte & 0xfeU) != 0) {
            throw std::invalid_argument("Avro OCF long is out of range");
        }
        encoded |= static_cast<std::uint64_t>(byte & 0x7fU)
                   << (byte_index * 7);
        if ((byte & 0x80U) == 0) {
            return static_cast<std::int64_t>(
                (encoded >> 1) ^
                (0U - static_cast<std::uint64_t>(encoded & 1U)));
        }
    }
    throw std::invalid_argument("Avro OCF long is out of range");
}

std::size_t checked_size(
    std::int64_t value,
    std::string_view description) {
    if (value < 0 ||
        static_cast<std::uint64_t>(value) >
            std::numeric_limits<std::size_t>::max()) {
        throw std::invalid_argument(
            "Invalid " + std::string(description) + " in Avro OCF");
    }
    return static_cast<std::size_t>(value);
}

void skip_bytes(
    const std::vector<std::uint8_t>& payload,
    std::size_t& offset,
    std::size_t size,
    std::string_view description) {
    if (size > payload.size() - offset) {
        throw std::invalid_argument(
            "Avro OCF ended while reading " + std::string(description));
    }
    offset += size;
}

void skip_length_prefixed_bytes(
    const std::vector<std::uint8_t>& payload,
    std::size_t& offset,
    std::string_view description) {
    const auto size = checked_size(
        decode_avro_long(payload, offset), description);
    skip_bytes(payload, offset, size, description);
}

std::array<std::uint8_t, 16> parse_ocf_header(
    const std::vector<std::uint8_t>& payload,
    std::size_t& offset) {
    if (payload.size() < 4 ||
        payload[0] != 'O' || payload[1] != 'b' ||
        payload[2] != 'j' || payload[3] != 1) {
        throw std::invalid_argument(
            "payload is not an Avro object container file");
    }
    offset = 4;

    while (true) {
        const std::int64_t encoded_count =
            decode_avro_long(payload, offset);
        if (encoded_count == 0) {
            break;
        }

        if (encoded_count < 0) {
            if (encoded_count == std::numeric_limits<std::int64_t>::min()) {
                throw std::invalid_argument(
                    "Avro OCF metadata item count is out of range");
            }
            const auto block_size = checked_size(
                decode_avro_long(payload, offset),
                "metadata block size");
            skip_bytes(
                payload, offset, block_size, "metadata block");
            continue;
        }

        const auto item_count =
            static_cast<std::uint64_t>(encoded_count);
        for (std::uint64_t index = 0; index < item_count; ++index) {
            skip_length_prefixed_bytes(
                payload, offset, "metadata key");
            skip_length_prefixed_bytes(
                payload, offset, "metadata value");
        }
    }

    std::array<std::uint8_t, 16> sync_marker{};
    if (sync_marker.size() > payload.size() - offset) {
        throw std::invalid_argument(
            "Avro OCF header is missing its sync marker");
    }
    std::copy_n(
        payload.begin() + static_cast<std::ptrdiff_t>(offset),
        sync_marker.size(),
        sync_marker.begin());
    offset += sync_marker.size();
    return sync_marker;
}

}  // namespace

std::vector<DynamicMapTick> make_dynamic_map_ticks() {
    const auto event_time = std::chrono::system_clock::now();
    std::vector<DynamicMapTick> ticks;
    ticks.reserve(5);

    auto add_tick = [&](std::string ticker,
                        double price,
                        std::string description,
                        std::map<std::string, MapValue> variablefields) {
        auto tick = make_stock_tick(
            std::move(ticker), price, event_time);
        tick.eventname = "dynamic Avro map ticks";
        tick.eventdesc = std::move(description);
        ticks.push_back(
            DynamicMapTick{
                .tick = std::move(tick),
                .variablefields = std::move(variablefields),
            });
    };

    add_tick(
        "MAP1",
        101.25,
        "Avro map record with 1 variable key",
        {{"venue", std::string("LSE")}});
    add_tick(
        "MAP2",
        202.50,
        "Avro map record with 2 variable keys",
        {
            {"bid", 202.45},
            {"isIndicative", false},
        });
    add_tick(
        "MAP3",
        303.75,
        "Avro map record with 3 variable keys",
        {
            {"currency", std::string("GBP")},
            {"tradePrice", 303.75},
            {"tradeSize", std::int64_t{25000}},
        });
    add_tick(
        "MAP4",
        404.00,
        "Avro map record with 4 variable keys",
        {
            {"auctionType", std::string("Closing")},
            {"imbalance", 1250.5},
            {"isClosingAuction", true},
            {"matchedVolume", std::int64_t{900000}},
        });
    add_tick(
        "MAP5",
        505.25,
        "Avro map record with 5 variable keys",
        {
            {"condition", std::string("AT")},
            {"isCorrection", false},
            {"note", std::monostate{}},
            {"sequenceNumber", std::int64_t{987654321}},
            {"yield", 4.125},
        });

    return ticks;
}

std::vector<std::uint8_t> serialize_dynamic_map_ticks_avro(
    const std::vector<DynamicMapTick>& ticks) {
    if (ticks.empty()) {
        throw std::invalid_argument("ticks must not be empty");
    }

    const avro::ValidSchema schema = compile_dynamic_map_schema();
    auto output = avro::memoryOutputStream();
    auto* output_view = output.get();
    avro::DataFileWriter<DynamicMapTick> writer(
        std::move(output), schema);
    for (const auto& tick : ticks) {
        writer.write(tick);
        writer.flush();
    }
    auto payload = *avro::snapshot(*output_view);
    writer.close();
    return payload;
}

std::vector<DynamicMapTick> deserialize_dynamic_map_ticks_avro(
    const std::vector<std::uint8_t>& payload) {
    if (payload.empty()) {
        throw std::invalid_argument("payload must not be empty");
    }

    const avro::ValidSchema schema = compile_dynamic_map_schema();
    auto input = avro::memoryInputStream(payload.data(), payload.size());
    avro::DataFileReader<DynamicMapTick> reader(
        std::move(input), schema);
    std::vector<DynamicMapTick> ticks;
    DynamicMapTick tick;
    while (reader.read(tick)) {
        ticks.push_back(std::move(tick));
        tick = {};
    }
    reader.close();
    return ticks;
}

std::size_t count_avro_ocf_data_blocks(
    const std::vector<std::uint8_t>& payload) {
    std::size_t offset = 0;
    const auto sync_marker = parse_ocf_header(payload, offset);
    std::size_t block_count = 0;

    while (offset < payload.size()) {
        const std::int64_t record_count =
            decode_avro_long(payload, offset);
        if (record_count <= 0) {
            throw std::invalid_argument(
                "Avro OCF data block must contain at least one record");
        }
        const auto block_size = checked_size(
            decode_avro_long(payload, offset), "data block size");
        skip_bytes(payload, offset, block_size, "data block");

        if (sync_marker.size() > payload.size() - offset ||
            !std::equal(
                sync_marker.begin(),
                sync_marker.end(),
                payload.begin() + static_cast<std::ptrdiff_t>(offset))) {
            throw std::invalid_argument(
                "Avro OCF data block has an invalid sync marker");
        }
        offset += sync_marker.size();
        ++block_count;
    }
    return block_count;
}

}  // namespace tickpoc
