#include "result.h"
#include "row.h"
#include <dbprove/sql/sql.h>
#include <iostream>

#include <vector>

#include "dbprove/sql/sql_exceptions.h"
#include <nlohmann/json.hpp>

#include "plog/Log.h"
#include <algorithm>
#include <cctype>

namespace sql::databricks
{
    using namespace nlohmann;

    bool starts_with(const std::string& value, const std::string_view prefix)
    {
        return value.size() >= prefix.size() && value.compare(0, prefix.size(), prefix) == 0;
    }

    std::string normalize_databricks_type(std::string type_name)
    {
        std::transform(type_name.begin(), type_name.end(), type_name.begin(), [](const unsigned char c) {
            return static_cast<char>(std::toupper(c));
        });
        return type_name;
    }

    SqlTypeKind map_databricks_type(const std::string& dbrick_type)
    {
        const auto normalized = normalize_databricks_type(dbrick_type);

        if (normalized == "SHORT" || normalized == "BYTE" || normalized == "TINYINT" || normalized == "SMALLINT") {
            return SqlTypeKind::SMALLINT;
        }
        if (normalized == "INT" || normalized == "INTEGER") {
            return SqlTypeKind::INT;
        }
        if (normalized == "LONG" || normalized == "BIGINT") {
            return SqlTypeKind::BIGINT;
        }
        if (normalized == "FLOAT" || normalized == "REAL") {
            return SqlTypeKind::REAL;
        }
        if (normalized == "DOUBLE") {
            return SqlTypeKind::DOUBLE;
        }
        if (starts_with(normalized, "DECIMAL")) {
            return SqlTypeKind::DECIMAL;
        }
        if (normalized == "DATE") {
            return SqlTypeKind::DATE;
        }
        if (normalized == "TIME") {
            return SqlTypeKind::TIME;
        }
        if (starts_with(normalized, "TIMESTAMP") || normalized == "DATETIME") {
            return SqlTypeKind::DATETIME;
        }
        if (normalized == "VOID" || normalized == "NULL") {
            return SqlTypeKind::SQL_NULL;
        }
        if (normalized == "STRING" ||
            starts_with(normalized, "VARCHAR") ||
            starts_with(normalized, "CHAR") ||
            normalized == "BOOLEAN" ||
            normalized == "BINARY" ||
            starts_with(normalized, "ARRAY") ||
            starts_with(normalized, "MAP") ||
            starts_with(normalized, "STRUCT") ||
            starts_with(normalized, "VARIANT") ||
            starts_with(normalized, "INTERVAL")) {
            return SqlTypeKind::STRING;
        }

        throw ProtocolException("Do not know how to map Databricks type: " + dbrick_type);
    }

    void Result::parseRows(const json& result)
    {
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
                if (row[colIdx].is_null()) {
                    currentRow.push_back(SqlVariant());
                    continue;
                }
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
                    currentRow.push_back(SqlVariant(SqlDecimal(row[colIdx].get<std::string>())));
                    break;
                case SqlTypeKind::DATE:
                case SqlTypeKind::TIME:
                case SqlTypeKind::DATETIME:
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
        : currentRow_(new Row(this, 0))
    {
        if (!response.contains("manifest")) {
            std::cerr << response.dump(2) << std::endl;
            throw ProtocolException("Expected to find a `manifest` in the json");
        }
        auto manifest = response["manifest"];
        const bool isTruncated = manifest["truncated"].get<bool>();
        if (isTruncated) {
            throw ProtocolException("Expected to get a non truncated stream when constructing from a single Json");
        }
        if (!manifest.contains("schema")) {
            throw ProtocolException("Expected to find a `schema` section in the json to locate data types");
        }
        const auto schema = manifest["schema"];

        if (!response.contains("result")) {
            throw ProtocolException("Expected to find a `result` in the json");
        }
        const auto result = response["result"];

        if (manifest.contains("total_row_count")) {
            rowCount_ = manifest["total_row_count"].get<int64_t>();
        } else {
            PLOGE << "Could not locate rowcount";
            PLOGE << response.dump(2);
        }


        for (auto& column : schema["columns"]) {
            auto dbrick_type_name = column["type_name"];
            columnTypes_.push_back(map_databricks_type(dbrick_type_name));
        }

        parseRows(result);
    }

    RowCount Result::rowCount() const
    {
        return rowCount_;
    }

    ColumnCount Result::columnCount() const
    {
        return columnTypes_.size();
    }

    const RowBase& Result::nextRow()
    {
        if (currentRowIndex_ < rowCount_) {
            currentRow_->currentRow_ = currentRowIndex_++;
            return *currentRow_;
        }
        return SentinelRow::instance();
    }

    void Result::reset()
    {
        currentRowIndex_ = 0;
    }
}
