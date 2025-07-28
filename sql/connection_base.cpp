#include "connection_base.h"

namespace sql {
const ConnectionBase::TypeMap& ConnectionBase::typeMap() const {
  static const TypeMap empty_map = {};
  return empty_map;
}

std::string ConnectionBase::mapTypes(std::string_view statement) const {
  std::string result(statement);

  for (const auto& [key, value] : typeMap()) {
    size_t pos = 0;

    while ((pos = statement.find(key, pos)) != std::string::npos) {
      result.replace(pos, key.length(), value);
      pos += value.length();
    }
  }
  return result;
}
}