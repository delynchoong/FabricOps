#pragma once

#include <filesystem>
#include <string>
#include <string_view>

namespace tickpoc {

void load_environment_file(
    const std::filesystem::path& path,
    bool required = false);

std::string require_environment_variable(std::string_view name);

}  // namespace tickpoc
