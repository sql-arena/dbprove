#include "connection_base.h"
#include "sql_exceptions.h"
#include "explain/plan.h"

namespace sql {
const ConnectionBase::TypeMap& ConnectionBase::typeMap() const {
  static const TypeMap empty_map = {};
  return empty_map;
}

std::unique_ptr<RowBase> ConnectionBase::fetchRow(const std::string_view statement) {
  const auto result = fetchAll(statement);
  if (result->rowCount() == 0) {
    throw EmptyResultException(statement);
  }
  if (result->rowCount() > 1) {
    throw InvalidRowsException(
        "Expected to find a single row in the data, but found: " + std::to_string(result->rowCount()),
        statement);
  }
  const auto& first = *result->rows().begin();
  return first.materialise();
}

SqlVariant ConnectionBase::fetchScalar(const std::string_view statement) {
  const auto row = fetchRow(statement);
  if (row->columnCount() != 1) {
    throw InvalidColumnsException("Expected to find a single column in the data", statement);
  }
  return row->asVariant(0);
}

std::unique_ptr<explain::Plan> ConnectionBase::explain(std::string_view statement) {
  return nullptr;
}

void ConnectionBase::createSchema(std::string_view schema_name) {
  execute("CREATE SCHEMA " + std::string(schema_name));
}

void ConnectionBase::analyse(std::string_view table_name) {
  execute("ANALYZE " + std::string(table_name));
}

std::optional<RowCount> ConnectionBase::tableRowCount(const std::string_view table) {
  const std::string dumb_row_count = "SELECT COUNT(*) FROM " + std::string(table);
  try {
    auto v = fetchScalar(dumb_row_count);
    return v.get<SqlBigInt>();
  } catch (InvalidObjectException&) {
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