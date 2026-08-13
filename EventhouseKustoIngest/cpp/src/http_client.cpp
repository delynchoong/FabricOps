#include "http_client.h"

#include <curl/curl.h>

#include <array>
#include <memory>
#include <mutex>
#include <random>
#include <stdexcept>

namespace tickpoc::detail {
namespace {

class CurlRuntime {
public:
    CurlRuntime() {
        if (curl_global_init(CURL_GLOBAL_DEFAULT) != CURLE_OK) {
            throw std::runtime_error("curl_global_init failed");
        }
    }

    ~CurlRuntime() { curl_global_cleanup(); }
};

void initialize_curl() {
    static CurlRuntime runtime;
    (void)runtime;
}

std::size_t append_response(
    char* data,
    std::size_t size,
    std::size_t count,
    void* destination) {
    const auto length = size * count;
    static_cast<std::string*>(destination)->append(data, length);
    return length;
}

std::unique_ptr<curl_slist, decltype(&curl_slist_free_all)> make_headers(
    const std::vector<std::string>& values) {
    curl_slist* raw_headers = nullptr;
    for (const auto& value : values) {
        curl_slist* next = curl_slist_append(raw_headers, value.c_str());
        if (next == nullptr) {
            curl_slist_free_all(raw_headers);
            throw std::runtime_error("Unable to allocate HTTP headers");
        }
        raw_headers = next;
    }
    return {raw_headers, &curl_slist_free_all};
}

}  // namespace

HttpResponse send_http_request(
    std::string_view method,
    const std::string& url,
    const std::vector<std::string>& headers,
    const std::vector<char>& body,
    long timeout_seconds) {
    initialize_curl();

    CURL* raw_curl = curl_easy_init();
    if (raw_curl == nullptr) {
        throw std::runtime_error("curl_easy_init failed");
    }
    std::unique_ptr<CURL, decltype(&curl_easy_cleanup)> curl(
        raw_curl, &curl_easy_cleanup);
    auto request_headers = make_headers(headers);
    const std::string method_text(method);

    HttpResponse response{};
    curl_easy_setopt(curl.get(), CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl.get(), CURLOPT_CUSTOMREQUEST, method_text.c_str());
    curl_easy_setopt(curl.get(), CURLOPT_HTTPHEADER, request_headers.get());
    curl_easy_setopt(curl.get(), CURLOPT_POSTFIELDS, body.data());
    curl_easy_setopt(
        curl.get(),
        CURLOPT_POSTFIELDSIZE_LARGE,
        static_cast<curl_off_t>(body.size()));
    curl_easy_setopt(curl.get(), CURLOPT_WRITEFUNCTION, append_response);
    curl_easy_setopt(curl.get(), CURLOPT_WRITEDATA, &response.body);
    curl_easy_setopt(curl.get(), CURLOPT_TIMEOUT, timeout_seconds);

    const CURLcode result = curl_easy_perform(curl.get());
    if (result != CURLE_OK) {
        throw std::runtime_error(
            std::string("HTTP request failed: ") +
            curl_easy_strerror(result));
    }
    curl_easy_getinfo(curl.get(), CURLINFO_RESPONSE_CODE, &response.status);
    return response;
}

void require_http_success(
    const HttpResponse& response,
    std::string_view operation) {
    if (response.status < 200 || response.status >= 300) {
        throw std::runtime_error(
            std::string(operation) + " returned HTTP " +
            std::to_string(response.status) +
            (response.body.empty() ? std::string{} : ": " + response.body));
    }
}

std::string trim_trailing_slashes(std::string value) {
    while (!value.empty() && value.back() == '/') {
        value.pop_back();
    }
    return value;
}

std::string url_encode(std::string_view value) {
    initialize_curl();

    char* encoded = curl_easy_escape(
        nullptr, value.data(), static_cast<int>(value.size()));
    if (encoded == nullptr) {
        throw std::runtime_error("Unable to URL-encode ingestion parameter");
    }
    std::string result(encoded);
    curl_free(encoded);
    return result;
}

std::string make_guid() {
    static thread_local std::mt19937_64 generator(std::random_device{}());
    std::array<unsigned char, 16> bytes{};
    for (auto& value : bytes) {
        value = static_cast<unsigned char>(generator());
    }
    bytes[6] = static_cast<unsigned char>((bytes[6] & 0x0f) | 0x40);
    bytes[8] = static_cast<unsigned char>((bytes[8] & 0x3f) | 0x80);

    static constexpr char hex[] = "0123456789abcdef";
    std::string value;
    value.reserve(36);
    for (std::size_t index = 0; index < bytes.size(); ++index) {
        if (index == 4 || index == 6 || index == 8 || index == 10) {
            value.push_back('-');
        }
        value.push_back(hex[bytes[index] >> 4]);
        value.push_back(hex[bytes[index] & 0x0f]);
    }
    return value;
}

}  // namespace tickpoc::detail
