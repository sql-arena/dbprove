#pragma once
#include <memory>
#include <span>
#include <string>
#include <vector>
#include <filesystem>

#include "sql_type.h"
#include "result_base.h"
#include "row_base.h"
#include "explain/node.h"


namespace sql {
class CredentialBase;
class Credential;

class ConnectionBase {
  bool closed_ = false;

public:
  virtual ~ConnectionBase() = default;

  explicit ConnectionBase(const CredentialBase& credential)
    : credential(credential) {
  };

  /// @brief Run statement and return
  virtual void execute(std::string_view statement) = 0;
  /// @brief Fetches a single results from the database.
  virtual std::unique_ptr<ResultBase> fetchAll(std::string_view statement) = 0;
  /// @brief Fetches multiple results from the data
  virtual std::unique_ptr<ResultBase> fetchMany(std::string_view statement) = 0;
  /// @brief Fetches a single row from the data.
  virtual std::unique_ptr<RowBase> fetchRow(std::string_view statement) = 0;
  /// @brief Fetches a single value from the database.
  virtual SqlVariant fetchValue(std::string_view statement) = 0;


  /**
   * @brief Run the query, discarding results and returning the query plan
   */
  virtual std::unique_ptr<const explain::Node> explain(std::string_view statement) { return nullptr; }

  /** @brief If bulk load API is availble, use that to load file.
   * @note If no bulk load API is available, the implementation must fall back to INSERT
   * and read the file manually
   * @param table to load
   * @param source_paths files used to input the load
   */
  virtual void bulkLoad(std::string_view table,
                        const std::vector<std::filesystem::path>& source_paths) = 0;
  /**
   * @brief Convenience method to bulk load data from a single file.
   * @param table
   * @param source_path
   */
  void bulkLoad(const std::string_view table, const std::filesystem::path& source_path) {
    const std::vector source_paths = {source_path};
    bulkLoad(table, source_paths);
  }

  /// @brief Creates a foreign key (if available)
  virtual void declareForeignKey(std::string_view fk_table,
                                 std::span<std::string_view> fk_columns,
                                 std::string_view pk_table,
                                 std::span<std::string_view> pk_columns) const {
  }

  /// @brief Given DDL, updates types to match the engine
  virtual std::string translateDialectDdl(const std::string_view ddl) const {
    return std::string(ddl);
  }

  const CredentialBase& credential;

  void close() { closed_ = true; };
};
}