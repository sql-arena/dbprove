#include "connection_base.h"

#include "sql_exceptions.h"

namespace sql {
const ConnectionBase::TypeMap& ConnectionBase::typeMap() const {
  static const TypeMap empty_map = {};
  return empty_map;
}

std::optional<RowCount> ConnectionBase::tableRowCount(const std::string_view table) {
  const std::string dumb_row_count = "SELECT COUNT(*) FROM " + std::string(table);
  try {
    auto v = fetchScalar(dumb_row_count);
    return v.get<SqlBigInt>();
  } catch (InvalidTableException&) {
    return std::nullopt;
  }
}

std::string ConnectionBase::mapTypes(std::string_view statement) const {
  std::string result(statement);

  for (const auto& [key, value] : typeMap()) {
    std::size_t pos = 0;
    while ((pos = result.find(key, pos)) != std::string::npos) {
      result.replace(pos, key.size(), value);
      pos += value.size();
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