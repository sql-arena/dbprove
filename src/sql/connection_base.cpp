#include "connection_base.h"

#include <iostream>
#include <regex>
#include <nlohmann/json.hpp>
#include <pugixml.hpp>
#include <cctype>
#include <algorithm>
#include <sstream>

#include "sql_exceptions.h"
#include "explain/plan.h"
#include "embedded_sql.h"
#include <fstream>
#include "plog/Log.h"

namespace sql {
namespace {
std::string normaliseExtension(std::string_view extension) {
  std::string normalised(extension);
  while (!normalised.empty() && normalised.front() == '.') {
    normalised.erase(normalised.begin());
  }
  return normalised;
}

std::string toLowerAscii(std::string value) {
  std::ranges::transform(value, value.begin(), [](const unsigned char c) {
    return static_cast<char>(std::tolower(c));
  });
  return value;
}

std::filesystem::path artefactPathWithDot(const std::filesystem::path& dir, std::string_view name, std::string_view extension) {
  const auto ext = normaliseExtension(extension);
  if (ext.empty()) {
    return dir / std::string(name);
  }
  return dir / (std::string(name) + "." + ext);
}

std::string prettyJsonIfNeeded(std::string_view extension, std::string_view content) {
  if (toLowerAscii(normaliseExtension(extension)) != "json") {
    return std::string(content);
  }
  try {
    return nlohmann::json::parse(content).dump(2);
  } catch (const std::exception& e) {
    PLOGW << "Artifact extension is json but payload was not valid JSON. Storing raw content. Error: " << e.what();
    return std::string(content);
  }
}

std::string prettyXmlIfNeeded(std::string_view extension, std::string_view content) {
  if (toLowerAscii(normaliseExtension(extension)) != "xml") {
    return std::string(content);
  }

  pugi::xml_document doc;
  const auto parse_result = doc.load_string(content.data());
  if (!parse_result) {
    PLOGW << "Artifact extension is xml but payload was not valid XML. Storing raw content. Error: " << parse_result.description();
    return std::string(content);
  }

  std::ostringstream out;
  doc.save(out, "  ", pugi::format_default, pugi::encoding_utf8);
  return out.str();
}

std::string prettyContentIfNeeded(std::string_view extension, std::string_view content) {
  const auto pretty_json = prettyJsonIfNeeded(extension, content);
  if (pretty_json != content) {
    return pretty_json;
  }
  return prettyXmlIfNeeded(extension, content);
}
}

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

std::unique_ptr<explain::Plan> ConnectionBase::explain(const std::string_view statement, std::optional<std::string_view> name) {
  return nullptr;
}

void ConnectionBase::executeDdl(const std::string_view ddl)
{
  const auto translatedDdl = translateDialectDdl(ddl);
  PLOGI << translatedDdl;
  execute(translatedDdl);
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
    const auto v = fetchScalar(dumb_row_count);
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
  } catch (NotImplementedException&) {
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
  try {
    auto all = fetchAll(sql);
    for (auto& row : all->rows()) {
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
  } catch (const std::exception&) {
    // If the engine does not support information schema introspection, skip type metadata.
    result.clear();
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

std::optional<std::string> ConnectionBase::getArtefact(const std::string_view name, const std::string_view extension) const {
  if (!artifacts_path_) {
    return std::nullopt;
  }

  const std::string engine_name = engine().internalName();
  const auto engine_dir = std::filesystem::path(*artifacts_path_) / engine_name;
  const auto path = artefactPathWithDot(engine_dir, name, extension);
  if (!std::filesystem::exists(path)) {
    return std::nullopt;
  }

  std::ifstream f(path);
  if (!f.is_open()) {
    return std::nullopt;
  }

  return std::string((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
}

void ConnectionBase::storeArtefact(const std::string_view name, const std::string_view extension, const std::string_view content) const {
  if (!artifacts_path_) {
    return;
  }

  const std::string engine_name = engine().internalName();
  const auto engine_dir = std::filesystem::path(*artifacts_path_) / engine_name;
  std::filesystem::create_directories(engine_dir);
  const auto path = artefactPathWithDot(engine_dir, name, extension);
  const auto formatted_content = prettyContentIfNeeded(extension, content);

  PLOGD << "Storing artefact to " << path.string();
  std::ofstream f(path);
  if (!f.is_open()) {
    PLOGE << "Failed to open artefact file for writing: " << path.string();
    return;
  }
  f << formatted_content;
}
}
