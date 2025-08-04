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

void ConnectionBase::validateSourcePaths(const std::vector<std::filesystem::path>& source_paths) {
  if (source_paths.empty()) {
    throw std::invalid_argument("No source paths provided for bulk load");
  }

  for (const auto& path : source_paths) {
    if (!std::filesystem::exists(path)) {
      throw std::runtime_error("CSV File does not exist: " + path.string());
    }
  }
}
}