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
  const json* root = &response;
  if (response.contains("result") && response["result"].is_object()) {
    root = &response["result"]; 
  }
  if (!root->contains("manifest")) {
    // Some responses (e.g. EXPLAIN output) may not include a manifest. If we still have
    // a data_array, infer a schema of STRING columns and proceed.
    if (root->contains("data_array") && (*root)["data_array"].is_array()) {
      size_t width = 0;
      if (!(*root)["data_array"].empty() && (*root)["data_array"][0].is_array()) {
        width = (*root)["data_array"][0].size();
      }
      if (width == 0) {
        // No columns and no manifest; treat as empty result set
        rowCount_ = 0;
        return;
      }
      columnTypes_.assign(width, SqlTypeKind::STRING);
      rowCount_ = (*root)["data_array"].size();
      parseRows(*root);
      return;
    }
    // Treat as an empty result to allow non-SELECT statements or unsupported responses to pass through
    rowCount_ = 0;
    return;
  }
  const auto& manifest = (*root)["manifest"];
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