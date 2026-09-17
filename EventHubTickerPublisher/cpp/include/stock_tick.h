#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace tickpoc {

struct StockTick {
    std::string eventname;
    std::int64_t eventtime;
    std::string ticker;
    double price;
    std::string eventdesc;
};

enum class AvroPayloadFormat {
    ObjectContainer,
    RawDatum,
};

StockTick make_stock_tick(
    std::string ticker,
    double price,
    std::chrono::system_clock::time_point event_time =
        std::chrono::system_clock::now());

std::vector<std::uint8_t> serialize_stock_tick_avro(const StockTick& tick);
StockTick deserialize_stock_tick_avro(
    const std::vector<std::uint8_t>& payload);
std::vector<std::uint8_t> serialize_stock_tick_avro_raw(
    const StockTick& tick);
StockTick deserialize_stock_tick_avro_raw(
    const std::vector<std::uint8_t>& payload);

class EventHubPublisher {
public:
    EventHubPublisher(
        std::string fully_qualified_namespace,
        std::string event_hub_name,
        AvroPayloadFormat payload_format =
            AvroPayloadFormat::ObjectContainer);
    ~EventHubPublisher();

    EventHubPublisher(EventHubPublisher&&) noexcept;
    EventHubPublisher& operator=(EventHubPublisher&&) noexcept;
    EventHubPublisher(const EventHubPublisher&) = delete;
    EventHubPublisher& operator=(const EventHubPublisher&) = delete;

    void publish(const std::vector<StockTick>& ticks);

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace tickpoc
