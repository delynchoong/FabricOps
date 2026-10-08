cmake_minimum_required(VERSION 3.20)
project(eventhub_ticker_producer LANGUAGES CXX)

include(CTest)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(WIN32 AND NOT TARGET avro-cpp::avrocpp_s)
    add_library(avro-cpp::avrocpp_s INTERFACE IMPORTED)
endif()

find_package(avro-cpp CONFIG REQUIRED)
find_package(azure-identity-cpp CONFIG REQUIRED)
find_package(azure-messaging-eventhubs-cpp CONFIG REQUIRED)

add_library(ticker_events
    src/dynamic_map_tick.cpp
    src/dynamic_tick.cpp
    src/environment_config.cpp
    src/stock_tick.cpp
    src/ticker_simulation.cpp
)
target_include_directories(ticker_events PUBLIC include)
target_compile_options(ticker_events PRIVATE $<$<CXX_COMPILER_ID:MSVC>:/utf-8>)
target_link_libraries(ticker_events
    PRIVATE
        avro-cpp::avrocpp
        Azure::azure-identity
        Azure::azure-messaging-eventhubs
)

add_executable(eventhub_ticker_producer src/main.cpp)
target_link_libraries(eventhub_ticker_producer PRIVATE ticker_events)

add_executable(eventhub_ticker_raw_producer src/raw_main.cpp)
target_link_libraries(eventhub_ticker_raw_producer PRIVATE ticker_events)

add_executable(eventhub_ticker_routing_producer src/routing_main.cpp)
target_link_libraries(eventhub_ticker_routing_producer
    PRIVATE
        ticker_events
        Azure::azure-identity
        Azure::azure-messaging-eventhubs
)

add_executable(
    eventhub_ticker_cross_database_routing_producer
    src/cross_database_routing_main.cpp
)
target_link_libraries(eventhub_ticker_cross_database_routing_producer
    PRIVATE
        ticker_events
        Azure::azure-identity
        Azure::azure-messaging-eventhubs
)

add_executable(
    eventhub_ticker_dynamic_producer
    src/dynamic_main.cpp
)
target_link_libraries(eventhub_ticker_dynamic_producer
    PRIVATE
        ticker_events
        Azure::azure-identity
        Azure::azure-messaging-eventhubs
)

add_executable(
    eventhub_ticker_dynamic_map_producer
    src/dynamic_map_main.cpp
)
target_link_libraries(eventhub_ticker_dynamic_map_producer
    PRIVATE
        ticker_events
        Azure::azure-identity
        Azure::azure-messaging-eventhubs
)

if(BUILD_TESTING)
    add_executable(ticker_events_tests tests/ticker_events_tests.cpp)
    target_link_libraries(ticker_events_tests PRIVATE ticker_events)
    add_test(NAME ticker_events_tests COMMAND ticker_events_tests)
endif()
