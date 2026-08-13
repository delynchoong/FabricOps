#include "environment_config.h"
#include "stock_tick_producer.h"

#include <cstdlib>
#include <exception>
#include <iostream>
#include <stdexcept>
#include <string>

int main(int argc, char* argv[]) {
    try {
        const std::string ticker = argc > 1 ? argv[1] : "MSFT";
        const double price = argc > 2 ? std::stod(argv[2]) : 420.00;
        const char* configured_env_file = std::getenv("TICKPOC_ENV_FILE");
        tickpoc::load_environment_file(
            configured_env_file == nullptr ? ".env" : configured_env_file,
            configured_env_file != nullptr);

        const tickpoc::IngestConfig config{
            .streaming_ingest_uri =
                tickpoc::require_environment_variable("KUSTO_QUERY_URI"),
            .database =
                tickpoc::require_environment_variable("KUSTO_DATABASE"),
            .table = tickpoc::require_environment_variable("KUSTO_TABLE"),
            .mapping = tickpoc::require_environment_variable("KUSTO_MAPPING"),
        };
        const std::string token =
            tickpoc::require_environment_variable("KUSTO_ACCESS_TOKEN");

        const auto event = tickpoc::make_stock_tick(ticker, price);
        tickpoc::ingest_stock_tick_avro(event, config, token);

        std::cout << "Ingested " << event.ticker << " at " << event.price
                  << '\n';
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
