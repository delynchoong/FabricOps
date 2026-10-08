#include "dynamic_tick.h"

// Avro C++ 1.12.1 uses fmt::format without including fmt/format.h.
#include <fmt/format.h>
#include <avro/Compiler.hh>
#include <avro/DataFile.hh>
#include <avro/Decoder.hh>
#include <avro/Encoder.hh>
#include <avro/Specific.hh>
#include <avro/Stream.hh>

#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>

namespace {

void encode_dynamic_value(
    avro::Encoder& encoder,
    const tickpoc::DynamicValue& value) {
    if (std::holds_alternative<std::monostate>(value)) {
        encoder.encodeUnionIndex(0);
        encoder.encodeNull();
    } else if (const auto* text = std::get_if<std::string>(&value)) {
        encoder.encodeUnionIndex(1);
        avro::encode(encoder, *text);
    } else {
        encoder.encodeUnionIndex(2);
        avro::encode(encoder, std::get<bool>(value));
    }
}

tickpoc::DynamicValue decode_dynamic_value(avro::Decoder& decoder) {
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
        default:
            throw std::runtime_error(
                "Unexpected Avro union branch for a dynamic field");
    }
}

}  // namespace

namespace avro {

template <>
struct codec_traits<tickpoc::DynamicTick> {
    static void encode(
        Encoder& encoder,
        const tickpoc::DynamicTick& value) {
        avro::encode(encoder, value.tick.eventname);
        avro::encode(encoder, value.tick.eventtime);
        avro::encode(encoder, value.tick.ticker);
        avro::encode(encoder, value.tick.price);
        avro::encode(encoder, value.tick.eventdesc);
        for (const auto& field : value.variablefields) {
            encode_dynamic_value(encoder, field);
        }
    }

    static void decode(Decoder& decoder, tickpoc::DynamicTick& value) {
        avro::decode(decoder, value.tick.eventname);
        avro::decode(decoder, value.tick.eventtime);
        avro::decode(decoder, value.tick.ticker);
        avro::decode(decoder, value.tick.price);
        avro::decode(decoder, value.tick.eventdesc);
        for (auto& field : value.variablefields) {
            field = decode_dynamic_value(decoder);
        }
    }
};

}  // namespace avro

namespace tickpoc {
namespace {

constexpr const char* kDynamicAvroSchema = R"({
  "type": "record",
  "name": "DynamicTick",
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
        "type": "record",
        "name": "VariableFields",
        "fields": [
          {"name": "variablefield1", "type": ["null", "string", "boolean"], "default": null},
          {"name": "variablefield2", "type": ["null", "string", "boolean"], "default": null},
          {"name": "variablefield3", "type": ["null", "string", "boolean"], "default": null},
          {"name": "variablefield4", "type": ["null", "string", "boolean"], "default": null},
          {"name": "variablefield5", "type": ["null", "string", "boolean"], "default": null},
          {"name": "variablefield6", "type": ["null", "string", "boolean"], "default": null},
          {"name": "variablefield7", "type": ["null", "string", "boolean"], "default": null},
          {"name": "variablefield8", "type": ["null", "string", "boolean"], "default": null},
          {"name": "variablefield9", "type": ["null", "string", "boolean"], "default": null},
          {"name": "variablefield10", "type": ["null", "string", "boolean"], "default": null}
        ]
      }
    }
  ]
})";

avro::ValidSchema compile_dynamic_schema() {
    std::istringstream schema_stream(kDynamicAvroSchema);
    avro::ValidSchema schema;
    avro::compileJsonSchema(schema_stream, schema);
    return schema;
}

}  // namespace

DynamicTick make_dynamic_tick(StockTick tick, int pattern) {
    if (pattern < 0) {
        throw std::invalid_argument("pattern must not be negative");
    }

    DynamicTick dynamic_tick{.tick = std::move(tick)};
    const int populated_fields = pattern % 10 + 1;
    for (int index = 0; index < populated_fields; ++index) {
        if ((pattern + index) % 2 == 0) {
            dynamic_tick.variablefields[index] =
                "value-" + std::to_string(pattern) + "-" +
                std::to_string(index + 1);
        } else {
            dynamic_tick.variablefields[index] =
                (pattern + index) % 3 == 0;
        }
    }
    return dynamic_tick;
}

std::vector<std::uint8_t> serialize_dynamic_tick_avro(
    const DynamicTick& tick) {
    const avro::ValidSchema schema = compile_dynamic_schema();
    auto output = avro::memoryOutputStream();
    auto* output_view = output.get();
    avro::DataFileWriter<DynamicTick> writer(std::move(output), schema);
    writer.write(tick);
    writer.flush();
    auto payload = *avro::snapshot(*output_view);
    writer.close();
    return payload;
}

DynamicTick deserialize_dynamic_tick_avro(
    const std::vector<std::uint8_t>& payload) {
    if (payload.empty()) {
        throw std::invalid_argument("payload must not be empty");
    }

    const avro::ValidSchema schema = compile_dynamic_schema();
    auto input = avro::memoryInputStream(payload.data(), payload.size());
    avro::DataFileReader<DynamicTick> reader(std::move(input), schema);
    DynamicTick tick;
    if (!reader.read(tick)) {
        throw std::runtime_error(
            "Avro payload did not contain a dynamic tick");
    }
    DynamicTick extra;
    if (reader.read(extra)) {
        throw std::runtime_error(
            "Avro payload must contain exactly one dynamic tick");
    }
    reader.close();
    return tick;
}

}  // namespace tickpoc
