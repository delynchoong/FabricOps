#include "stock_tick.h"

#include <azure/identity.hpp>
#include <azure/messaging/eventhubs.hpp>

// Avro C++ 1.12.1 uses fmt::format without including fmt/format.h.
#include <fmt/format.h>
#include <avro/Compiler.hh>
#include <avro/DataFile.hh>
#include <avro/Encoder.hh>
#include <avro/Generic.hh>
#include <avro/Specific.hh>
#include <avro/Stream.hh>

#include <chrono>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>

namespace avro {

template <>
struct codec_traits<tickpoc::StockTick> {
    static void encode(Encoder& encoder, const tickpoc::StockTick& value) {
        avro::encode(encoder, value.eventname);
        avro::encode(encoder, value.eventtime);
        avro::encode(encoder, value.ticker);
        avro::encode(encoder, value.price);
        avro::encode(encoder, value.eventdesc);
    }

    static void decode(Decoder& decoder, tickpoc::StockTick& value) {
        avro::decode(decoder, value.eventname);
        avro::decode(decoder, value.eventtime);
        avro::decode(decoder, value.ticker);
        avro::decode(decoder, value.price);
        avro::decode(decoder, value.eventdesc);
    }
};

}  // namespace avro

namespace tickpoc {
namespace {

constexpr const char* kAvroSchema = R"({
  "type": "record",
  "name": "StockTick",
  "namespace": "tickpoc",
  "fields": [
    {"name": "eventname", "type": "string"},
    {"name": "eventtime", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "ticker", "type": "string"},
    {"name": "price", "type": "double"},
    {"name": "eventdesc", "type": "string"}
  ]
})";

class TemporaryFile {
public:
    TemporaryFile() {
        const auto nonce = std::chrono::steady_clock::now()
                               .time_since_epoch()
                               .count();
        path_ = std::filesystem::temp_directory_path() /
                ("eventhub-tick-" + std::to_string(nonce) + ".avro");
    }

    ~TemporaryFile() {
        std::error_code error;
        std::filesystem::remove(path_, error);
    }

    const std::filesystem::path& path() const { return path_; }

private:
    std::filesystem::path path_;
};

}  // namespace

StockTick make_stock_tick(
    std::string ticker,
    double price,
    std::chrono::system_clock::time_point event_time) {
    if (ticker.empty()) {
        throw std::invalid_argument("ticker must not be empty");
    }
    if (!std::isfinite(price) || price < 0.0) {
        throw std::invalid_argument(
            "price must be finite and must not be negative");
    }

    return StockTick{
        .eventname = "stock ticks",
        .eventtime = std::chrono::duration_cast<std::chrono::milliseconds>(
                         event_time.time_since_epoch())
                         .count(),
        .ticker = std::move(ticker),
        .price = price,
        .eventdesc = "stock ticker price",
    };
}

std::vector<std::uint8_t> serialize_stock_tick_avro(const StockTick& tick) {
    std::istringstream schema_stream(kAvroSchema);
    avro::ValidSchema schema;
    avro::compileJsonSchema(schema_stream, schema);

    TemporaryFile file;
    {
        avro::DataFileWriter<StockTick> writer(
            file.path().string().c_str(), schema);
        writer.write(tick);
        writer.close();
    }

    std::ifstream input(file.path(), std::ios::binary);
    if (!input) {
        throw std::runtime_error("Unable to read generated Avro file");
    }
    return {std::istreambuf_iterator<char>(input),
            std::istreambuf_iterator<char>()};
}

StockTick deserialize_stock_tick_avro(
    const std::vector<std::uint8_t>& payload) {
    if (payload.empty()) {
        throw std::invalid_argument("payload must not be empty");
    }

    TemporaryFile file;
    {
        std::ofstream output(file.path(), std::ios::binary);
        if (!output) {
            throw std::runtime_error("Unable to create temporary Avro file");
        }
        output.write(
            reinterpret_cast<const char*>(payload.data()),
            static_cast<std::streamsize>(payload.size()));
    }

    avro::DataFileReader<StockTick> reader(file.path().string().c_str());
    StockTick tick;
    if (!reader.read(tick)) {
        throw std::runtime_error("Avro payload did not contain a stock tick");
    }
    StockTick extra;
    if (reader.read(extra)) {
        throw std::runtime_error(
            "Avro payload must contain exactly one stock tick");
    }
    reader.close();
    return tick;
}

std::vector<std::uint8_t> serialize_stock_tick_avro_raw(
    const StockTick& tick) {
    auto output = avro::memoryOutputStream();
    const auto encoder = avro::binaryEncoder();
    encoder->init(*output);
    avro::encode(*encoder, tick);
    encoder->flush();

    return *avro::snapshot(*output);
}

StockTick deserialize_stock_tick_avro_raw(
    const std::vector<std::uint8_t>& payload) {
    if (payload.empty()) {
        throw std::invalid_argument("payload must not be empty");
    }

    auto input = avro::memoryInputStream(payload.data(), payload.size());
    const auto decoder = avro::binaryDecoder();
    decoder->init(*input);
    StockTick tick;
    avro::decode(*decoder, tick);
    return tick;
}

class EventHubProducer::Impl {
public:
    Impl(
        std::string fully_qualified_namespace,
        std::string event_hub_name,
        AvroPayloadFormat payload_format)
        : producer(
              std::move(fully_qualified_namespace),
              std::move(event_hub_name),
              std::make_shared<Azure::Identity::DefaultAzureCredential>()),
          payload_format(payload_format) {}

    void publish(const std::vector<StockTick>& ticks) {
        if (ticks.empty()) {
            throw std::invalid_argument("ticks must not be empty");
        }

        auto batch = producer.CreateBatch();
        for (const auto& tick : ticks) {
            Azure::Messaging::EventHubs::Models::EventData event;
            if (payload_format == AvroPayloadFormat::RawDatum) {
                event.Body = serialize_stock_tick_avro_raw(tick);
                event.ContentType = "application/octet-stream";
                event.Properties["avro.encoding"] =
                    Azure::Core::Amqp::Models::AmqpValue("raw-datum");
            } else {
                event.Body = serialize_stock_tick_avro(tick);
                event.ContentType = "avro/binary";
            }
            event.MessageId = Azure::Core::Amqp::Models::AmqpValue(
                tick.ticker + "-" + std::to_string(tick.eventtime));
            event.Properties["avro.schema.name"] =
                Azure::Core::Amqp::Models::AmqpValue("tickpoc.StockTick");
            if (!batch.TryAdd(event)) {
                if (batch.NumberOfEvents() == 0) {
                    throw std::runtime_error(
                        "A stock tick exceeds the Event Hubs batch size");
                }
                producer.Send(batch);
                batch = producer.CreateBatch();
                if (!batch.TryAdd(event)) {
                    throw std::runtime_error(
                        "A stock tick exceeds the Event Hubs batch size");
                }
            }
        }
        if (batch.NumberOfEvents() > 0) {
            producer.Send(batch);
        }
    }

private:
    Azure::Messaging::EventHubs::ProducerClient producer;
    AvroPayloadFormat payload_format;
};

EventHubProducer::EventHubProducer(
    std::string fully_qualified_namespace,
    std::string event_hub_name,
    AvroPayloadFormat payload_format) {
    if (fully_qualified_namespace.empty()) {
        throw std::invalid_argument(
            "fully_qualified_namespace must not be empty");
    }
    if (event_hub_name.empty()) {
        throw std::invalid_argument("event_hub_name must not be empty");
    }
    impl_ = std::make_unique<Impl>(
        std::move(fully_qualified_namespace),
        std::move(event_hub_name),
        payload_format);
}

EventHubProducer::~EventHubProducer() = default;
EventHubProducer::EventHubProducer(EventHubProducer&&) noexcept = default;
EventHubProducer& EventHubProducer::operator=(
    EventHubProducer&&) noexcept = default;

void EventHubProducer::publish(const std::vector<StockTick>& ticks) {
    impl_->publish(ticks);
}

}  // namespace tickpoc
