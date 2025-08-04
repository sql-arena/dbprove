#pragma once
#include <Engine.h>

#include <memory>
#include <span>
#include <string>
#include <vector>
#include <filesystem>
#include <map>

#include "credential.h"
#include "sql_type.h"
#include "result_base.h"
#include "row_base.h"
#include "explain/plan.h"


namespace sql {

class ConnectionBase {
  bool closed_ = false;
  const Engine engine_;
public:
  using TypeMap = std::map<std::string_view, std::string_view>;
  virtual ~ConnectionBase() = default;

  explicit ConnectionBase(const Credential& credential, const Engine& engine)
    : engine_(engine), credential(credential) {
  };

  /// @brief Used to map type names specific to the engine before executing DDL/SQL
  virtual const TypeMap& typeMap() const;
  const Engine& engine() const { return engine_; }

  /// @brief Run statement and return
  virtual void execute(std::string_view statement) = 0;
  /// @brief Fetches a single results from the database.
  virtual std::unique_ptr<ResultBase> fetchAll(std::string_view statement) = 0;
  /// @brief Fetches multiple results from the data
  virtual std::unique_ptr<ResultBase> fetchMany(std::string_view statement) = 0;
  /// @brief Fetches a single row from the data.
  /// @throw If the statement does not return a single row
  virtual std::unique_ptr<RowBase> fetchRow(std::string_view statement) = 0;
  /// @brief Fetches a single, scalar value from the database.
  /// @throw If the statement does not return a single row with a single column
  virtual SqlVariant fetchScalar(std::string_view statement) = 0;

  /**
   * @brief Run the query, discarding results and returning the query plan
   */
  virtual std::unique_ptr<explain::Plan> explain(std::string_view statement) { return nullptr; }

  /** @brief If bulk load API is available, use that to load file.
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

  const Credential& credential;

  void close() { closed_ = true; };

  std::string mapTypes(std::string_view statement) const;
protected:
  static void validateSourcePaths(const std::vector<std::filesystem::path>& source_paths);
};


}