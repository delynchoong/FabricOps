#include "ticker_simulation.h"

int main(int argc, char* argv[]) {
    return tickpoc::run_ticker_simulation(
        argc,
        argv,
        tickpoc::AvroPayloadFormat::ObjectContainer,
        "eventhub_ticker_publisher");
}
