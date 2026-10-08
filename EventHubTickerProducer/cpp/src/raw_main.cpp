#include "ticker_simulation.h"

#include <iostream>

// Negative compatibility test only. Fabric Eventhouse's validated direct Avro
// connection requires a complete Object Container File with Obj\x01 magic and
// an embedded writer schema. This executable intentionally sends only a raw
// datum so Eventhouse rejects it with "wrong magic in header".
int main(int argc, char* argv[]) {
    std::cerr
        << "WARNING: raw Avro datum is an intentional unsupported test; "
           "use an OCF producer for Fabric Eventhouse ingestion.\n";
    return tickpoc::run_ticker_simulation(
        argc,
        argv,
        tickpoc::AvroPayloadFormat::RawDatum,
        "eventhub_ticker_raw_producer");
}
