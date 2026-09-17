#include "ticker_simulation.h"

int main(int argc, char* argv[]) {
    return tickpoc::run_ticker_simulation(
        argc,
        argv,
        tickpoc::AvroPayloadFormat::RawDatum,
        "eventhub_ticker_raw_publisher");
}
