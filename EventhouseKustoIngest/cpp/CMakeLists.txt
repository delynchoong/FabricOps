cmake_minimum_required(VERSION 3.20)
project(eventhouse_kusto_ingest LANGUAGES CXX)

include(CTest)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(WIN32 AND NOT TARGET avro-cpp::avrocpp_s)
    add_library(avro-cpp::avrocpp_s INTERFACE IMPORTED)
endif()

find_package(avro-cpp CONFIG REQUIRED)
find_package(CURL CONFIG REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)

add_library(stock_tick_producer
    src/environment_config.cpp
    src/http_client.cpp
    src/queued_ingestion.cpp
    src/stock_tick_producer.cpp
)
target_include_directories(stock_tick_producer PUBLIC include)
target_compile_options(stock_tick_producer PRIVATE $<$<CXX_COMPILER_ID:MSVC>:/utf-8>)
target_link_libraries(stock_tick_producer
    PRIVATE
        avro-cpp::avrocpp
        CURL::libcurl
        nlohmann_json::nlohmann_json
)

add_executable(stock_tick_demo src/main.cpp)
target_link_libraries(stock_tick_demo PRIVATE stock_tick_producer)

add_executable(stock_ticker_generator src/stock_ticker_generator.cpp)
target_link_libraries(stock_ticker_generator PRIVATE stock_tick_producer)

if(BUILD_TESTING)
    add_executable(
        stock_tick_ingestion_tests
        tests/stock_tick_ingestion_tests.cpp
    )
    target_include_directories(
        stock_tick_ingestion_tests
        PRIVATE
            src
    )
    target_link_libraries(
        stock_tick_ingestion_tests
        PRIVATE
            stock_tick_producer
    )
    add_test(NAME stock_tick_ingestion_tests COMMAND stock_tick_ingestion_tests)
endif()
