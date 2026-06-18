#pragma once

#include <filesystem>
#include <string>
#include <string_view>
#include <stdexcept>

namespace dbprove::common {
struct QualifiedTableName {
  std::string schema_name;
  std::string table_name;
};

inline constexpr std::string_view kTableDataDirectoryName = "table_data";
inline constexpr std::string_view kClickHouseContainerTableDataRoot = "/var/lib/clickhouse/user_files/table_data";
inline constexpr std::string_view kMssqlContainerTableDataRoot = "/var/opt/mssql/table_data";
inline constexpr std::string_view kMountedTableDataRoot = "/opt/table-data-source";

inline QualifiedTableName splitQualifiedTableName(std::string_view qualified_table_name) {
  const auto delimiter = qualified_table_name.find('.');
  if (delimiter == std::string_view::npos) {
    return QualifiedTableName{std::string(), std::string(qualified_table_name)};
  }

  const auto schema = qualified_table_name.substr(0, delimiter);
  const auto table = qualified_table_name.substr(delimiter + 1);
  if (schema.empty() || table.empty()) {
    throw std::invalid_argument("Schema and table names cannot be empty.");
  }
  return QualifiedTableName{std::string(schema), std::string(table)};
}

inline std::string schemaObjectPath(std::string_view schema_name) {
  std::string path(schema_name);
  for (auto& c : path) {
    if (c == '_') {
      c = '/';
    }
  }
  return path;
}

inline std::string qualifyRegisteredTableName(std::string_view table_name, std::string_view dataset_name) {
  if (table_name.contains('.')) {
    return std::string(table_name);
  }
  return std::string(dataset_name) + "." + std::string(table_name);
}

inline std::string tableFileStem(std::string_view qualified_table_name, const size_t file_index, const size_t file_count) {
  const auto split = splitQualifiedTableName(qualified_table_name);
  if (file_count <= 1) {
    return split.table_name;
  }
  return split.table_name + "_" + std::format("{:04}", file_index + 1);
}

inline std::filesystem::path stagedTablePath(const std::filesystem::path& base_path,
                                             std::string_view qualified_table_name,
                                             std::string_view file_name) {
  const auto split = splitQualifiedTableName(qualified_table_name);
  if (split.schema_name.empty()) {
    return base_path / std::string(file_name);
  }
  return base_path / split.schema_name / std::string(file_name);
}

inline std::filesystem::path stagedMountedParquetPath(std::string_view qualified_table_name,
                                                      const size_t file_index,
                                                      const size_t file_count) {
  const auto split = splitQualifiedTableName(qualified_table_name);
  const auto stem = tableFileStem(qualified_table_name, file_index, file_count) + ".parquet";
  if (split.schema_name.empty()) {
    return std::filesystem::path(kMountedTableDataRoot) / stem;
  }
  return std::filesystem::path(kMountedTableDataRoot) / split.schema_name / stem;
}

inline std::filesystem::path stagedContainerPath(std::string_view container_root,
                                                 std::string_view qualified_table_name,
                                                 std::string_view file_name) {
  const auto split = splitQualifiedTableName(qualified_table_name);
  if (split.schema_name.empty()) {
    return std::filesystem::path(container_root) / std::string(file_name);
  }
  return std::filesystem::path(container_root) / split.schema_name / std::string(file_name);
}
}
