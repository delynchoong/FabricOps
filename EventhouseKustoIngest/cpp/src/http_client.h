#pragma once

#include <string>
#include <string_view>
#include <vector>

namespace tickpoc::detail {

struct HttpResponse {
    long status;
    std::string body;
};

HttpResponse send_http_request(
    std::string_view method,
    const std::string& url,
    const std::vector<std::string>& headers,
    const std::vector<char>& body,
    long timeout_seconds);

void require_http_success(
    const HttpResponse& response,
    std::string_view operation);

std::string trim_trailing_slashes(std::string value);
std::string url_encode(std::string_view value);
std::string make_guid();

}  // namespace tickpoc::detail
