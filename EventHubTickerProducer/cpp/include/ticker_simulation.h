#pragma once

#include "stock_tick.h"

namespace tickpoc {

int run_ticker_simulation(
    int argc,
    char* argv[],
    AvroPayloadFormat payload_format,
    const char* executable_name);

}  // namespace tickpoc
