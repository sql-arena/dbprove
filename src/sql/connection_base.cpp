#include "connection_base.h"

#include <regex>

#include "sql_exceptions.h"
#include "explain/plan.h"
#include "embedded_sql.h"

namespace sql {
const ConnectionBase::TypeMap& ConnectionBase::typeMap() const {
  static const TypeMap empty_map = {};
  return empty_map;
}

std::unique_ptr<RowBase> ConnectionBase::fetchRow(const std::string_view statement) {
  const auto result = fetchAll(statement);
  std::unique_ptr<MaterialisedRow> first_row = nullptr;
  for (auto& row : result->rows()) {
    if (first_row) {
      throw InvalidRowsException("Expected to find a single row in the data, but found more than one", statement);
    }
    first_row = std::move(row.materialise());
  }
  if (!first_row) {
    throw EmptyResultException(statement);
  }
  return first_row;
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


void ConnectionBase::declareForeignKey(const std::string_view fk_table, const std::span<std::string_view> fk_columns,
                                       const std::string_view pk_table, const std::span<std::string_view> pk_columns) {
  const std::string statement = "ALTER TABLE " + std::string(fk_table) + " ADD CONSTRAINT " + foreignKeyName(fk_table) +
                                " FOREIGN KEY (" + join(fk_columns, ", ") + ")" + " REFERENCES " + std::string(pk_table)
                                + "(" + join(pk_columns, ", ") + ")";

  try {
    execute(statement);
  } catch (InvalidObjectException&) {
    /* NOOP */
  }
}

std::string ConnectionBase::foreignKeyName(std::string_view table_name) {
  return "fk_" + std::regex_replace(std::string(table_name), std::regex("\\."), "_");
}


std::vector<SqlTypeMeta> ConnectionBase::describeColumnTypes(std::string_view table) {
  const auto [schema_name, table_name] = splitTable(table);
  std::string sql = std::regex_replace(std::string(resource::data_type_sql), std::regex("\\{table_name\\}"),
                                       table_name);
  sql = std::regex_replace(sql, std::regex("\\{schema_name\\}"), schema_name);

  std::vector<SqlTypeMeta> result;
  for (auto& row : fetchAll(sql)->rows()) {
    auto engine_type = to_upper(row[0].asString());
    const auto sql_type = to_sql_type_kind(engine_type);
    switch (sql_type) {
      case SqlTypeKind::STRING: {
        const auto length = row[1].asInt8();
        result.push_back(SqlTypeMeta{sql_type, SqlTypeModifier(length)});
        break;
      }
      default:
        result.push_back(SqlTypeMeta{sql_type, SqlTypeModifier()});
        break;
    }
  }
  return result;
}

std::string ConnectionBase::mapTypes(const std::string_view statement) const {
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