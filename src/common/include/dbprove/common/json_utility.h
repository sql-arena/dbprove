#pragma once

#include <nlohmann/json_fwd.hpp>

#include <cstdint>
#include <optional>
#include <string>

namespace dbprove::common {
std::optional<int64_t> json_as_int64(const nlohmann::json& value);
std::optional<double> json_as_double(const nlohmann::json& value);
std::string json_decimal128_to_string(const nlohmann::json& decimal);
std::string json_date32_to_string(int64_t days_since_epoch);
std::string json_arrow_type_to_string(const nlohmann::json& arrow_type);
} // namespace dbprove::common
