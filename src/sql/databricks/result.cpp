#include "result.h"
#include "row.h"
#include <dbprove/sql/sql.h>
#include <iostream>

#include <vector>

#include "dbprove/sql/sql_exceptions.h"
#include <nlohmann/json.hpp>

namespace sql::databricks {
using namespace nlohmann;


SqlTypeKind map_databricks_type(const std::string& dbrick_type) {
  static std::map<std::string, SqlTypeKind> m = {
      {"INT", SqlTypeKind::INT},
      {"STRING", SqlTypeKind::STRING},
  };
  if (!m.contains(dbrick_type)) {
    throw ProtocolException("Do not know how to map Databricks type: " + dbrick_type);
  }
  return m[dbrick_type];
}

void Result::parseRows(const json& result) {
  if (!result.contains("data_array") || !result["data_array"].is_array()) {
    // Instead of an empty array, Databricks returns an absent array
    return;
  }
  for (auto& row : result["data_array"]) {
    rows_.push_back(std::vector<SqlVariant>());
    rows_.back().reserve(row.size());
    auto& currentRow = rows_.back();
    if (!row.is_array()) {
      throw ProtocolException("Invalid data. Each element must be an array.");
    }

    for (size_t colIdx = 0; colIdx < row.size(); ++colIdx) {
      switch (columnTypes_[colIdx]) {
        case SqlTypeKind::TINYINT:
          currentRow.push_back(SqlVariant(static_cast<int8_t>(std::stoi(row[colIdx].get<std::string>()))));
          break;
        case SqlTypeKind::SMALLINT:
          currentRow.push_back(SqlVariant(static_cast<int16_t>(std::stoi(row[colIdx].get<std::string>()))));
          break;
        case SqlTypeKind::INT:
          currentRow.push_back(SqlVariant(static_cast<int32_t>(std::stoi(row[colIdx].get<std::string>()))));
          break;
        case SqlTypeKind::BIGINT:
          currentRow.push_back(SqlVariant(static_cast<int64_t>(std::stoll(row[colIdx].get<std::string>()))));
          break;
        case SqlTypeKind::REAL:
          currentRow.push_back(SqlVariant(static_cast<float>(std::stof(row[colIdx].get<std::string>()))));
          break;
        case SqlTypeKind::DOUBLE:
          currentRow.push_back(SqlVariant(static_cast<double>(std::stod(row[colIdx].get<std::string>()))));
          break;
        case SqlTypeKind::DECIMAL:
          // For DECIMAL type, assuming you store as string for precision arithmetic
          currentRow.push_back(SqlVariant(row[colIdx].get<std::string>()));
          break;
        case SqlTypeKind::STRING:
          currentRow.push_back(SqlVariant(row[colIdx].get<std::string>()));
          break;
        case SqlTypeKind::SQL_NULL:
          currentRow.push_back(SqlVariant());
          break;
        default:
          throw ProtocolException("Unexpected SQL type encountered.");
      }
    }
  }
}

Result::Result(const json& response)
  : currentRow_(new Row(this, 0)) {
  if (!response.contains("manifest")) {
    throw ProtocolException("Expected to find a `manifest` string in the returned json");
  }
  const auto manifest = response["manifest"];
  const bool isTruncated = manifest["truncated"].get<bool>();
  if (isTruncated) {
    throw ProtocolException("Expected to get a non truncated stream when constructing from a single Json");
  }
  if (!manifest.contains("schema")) {
    throw ProtocolException("Expected to find a `schema` section in the json to locate data types");
  }
  const auto schema = manifest["schema"];

  rowCount_ += manifest["total_row_count"].get<int64_t>();
  for (auto& column : schema["columns"]) {
    auto dbrick_type_name = column["type_name"];
    columnTypes_.push_back(map_databricks_type(dbrick_type_name));
  }

  if (!response.contains("result")) {
    throw ProtocolException("Expected to find a `result` string in the json");
  }
  const auto result = response["result"];

  parseRows(result);
}

RowCount Result::rowCount() const {
  return rowCount_;
}

ColumnCount Result::columnCount() const {
  return columnTypes_.size();
}

const RowBase& Result::nextRow() {
  if (currentRowIndex_ > rowCount_) {
    return SentinelRow::instance();
  }
  currentRow_->currentRow_ = currentRowIndex_++;
  return *currentRow_;
}
}