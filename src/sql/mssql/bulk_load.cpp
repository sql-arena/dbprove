#include <iostream>
#include "connection.h"
#include <sql.h>
#include <sqlext.h>
#include <filesystem>
#include <string>
#include <vector>
#include <cstdlib>
#include <plog/Log.h>
#include <dbprove/common/string.h>

namespace sql::mssql {

void Connection::bulkLoad(const std::string_view table, const std::vector<std::filesystem::path> source_paths) {
  validateSourcePaths(source_paths);

  for (const auto& path : source_paths) {
    PLOGI << "Bulk loading file: " << path << " into table: " << table;

    // Translate host path to container path
    // We assume the host's run/table_data is mounted to /var/opt/mssql/table_data
    std::string filename = path.filename().string();
    std::string container_path = "/var/opt/mssql/table_data/" + filename;

    // Construct the BULK INSERT command
    // FORMAT = 'CSV' is supported in SQL Server 2017+
    // FIELDQUOTE = '"' handles the quoted strings that BCP couldn't handle
    std::string sql = "BULK INSERT " + std::string(table) + " FROM '" + container_path + "' WITH (";
    sql += "FORMAT = 'CSV', ";
    sql += "FIELDTERMINATOR = '|', ";
    sql += "FIELDQUOTE = '\"', ";
    sql += "ROWTERMINATOR = '0x0a', "; // Use hex for \n to be safe
    sql += "FIRSTROW = 2, ";
    sql += "TABLOCK";
    sql += ")";

    PLOGI << "Executing BULK INSERT: " << sql;

    try {
        execute(sql);
    } catch (const std::exception& e) {
        PLOGE << "BULK INSERT failed: " << e.what();
        throw;
    }

    PLOGI << "BULK INSERT completed successfully for table: " << table;
  }
}

} // namespace sql::mssql
