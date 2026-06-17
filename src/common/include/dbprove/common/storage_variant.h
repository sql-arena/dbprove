#pragma once

#include <string_view>

namespace dbprove {
enum class StorageVariant : unsigned char {
  Native,
  Iceberg
};

constexpr inline std::string_view to_string(const StorageVariant variant) {
  return variant == StorageVariant::Native ? "native" : "iceberg";
}
}
