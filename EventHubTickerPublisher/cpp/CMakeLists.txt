cmake_minimum_required(VERSION 3.20)
project(eventhub_ticker_publisher LANGUAGES CXX)

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

add_executable(eventhub_ticker_publisher src/main.cpp)
target_link_libraries(eventhub_ticker_publisher PRIVATE ticker_events)

add_executable(eventhub_ticker_raw_publisher src/raw_main.cpp)
target_link_libraries(eventhub_ticker_raw_publisher PRIVATE ticker_events)

if(BUILD_TESTING)
    add_executable(ticker_events_tests tests/ticker_events_tests.cpp)
    target_link_libraries(ticker_events_tests PRIVATE ticker_events)
    add_test(NAME ticker_events_tests COMMAND ticker_events_tests)
endif()
