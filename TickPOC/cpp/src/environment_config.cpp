#include "environment_config.h"

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <fstream>
#include <stdexcept>
#include <string>

namespace {

std::string trim(std::string value) {
    const auto not_space = [](unsigned char character) {
        return !std::isspace(character);
    };
    const auto first = std::find_if(value.begin(), value.end(), not_space);
    const auto last = std::find_if(value.rbegin(), value.rend(), not_space).base();
    if (first >= last) {
        return {};
    }
    return std::string(first, last);
}

void set_environment_variable(
    const std::string& name,
    const std::string& value) {
#ifdef _WIN32
    if (_putenv_s(name.c_str(), value.c_str()) != 0) {
        throw std::runtime_error(
            "Unable to set environment variable " + name);
    }
#else
    if (setenv(name.c_str(), value.c_str(), 0) != 0) {
        throw std::runtime_error(
            "Unable to set environment variable " + name);
    }
#endif
}

}  // namespace

namespace tickpoc {

void load_environment_file(
    const std::filesystem::path& path,
    bool required) {
    std::ifstream input(path);
    if (!input) {
        if (required) {
            throw std::runtime_error(
                "Unable to open environment file: " + path.string());
        }
        return;
    }

    std::string line;
    std::size_t line_number = 0;
    while (std::getline(input, line)) {
        ++line_number;
        const std::string entry = trim(line);
        if (entry.empty() || entry.front() == '#') {
            continue;
        }

        const std::size_t separator = entry.find('=');
        if (separator == std::string::npos) {
            throw std::runtime_error(
                "Invalid environment entry at " + path.string() + ":" +
                std::to_string(line_number));
        }

        const std::string name = trim(entry.substr(0, separator));
        std::string value = trim(entry.substr(separator + 1));
        if (name.empty()) {
            throw std::runtime_error(
                "Empty environment variable name at " + path.string() + ":" +
                std::to_string(line_number));
        }
        if (value.size() >= 2 &&
            ((value.front() == '"' && value.back() == '"') ||
             (value.front() == '\'' && value.back() == '\''))) {
            value = value.substr(1, value.size() - 2);
        }

        if (std::getenv(name.c_str()) == nullptr) {
            set_environment_variable(name, value);
        }
    }
}

std::string require_environment_variable(std::string_view name) {
    const std::string key(name);
    const char* value = std::getenv(key.c_str());
    if (value == nullptr || *value == '\0') {
        throw std::runtime_error(key + " is required");
    }
    return value;
}

}  // namespace tickpoc
