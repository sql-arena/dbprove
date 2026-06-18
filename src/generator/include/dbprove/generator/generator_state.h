#pragma once
#include <dbprove/sql/sql.h>
#include <dbprove/common/cloud_provider.h>
#include <dbprove/common/storage_variant.h>
#include <filesystem>
#include <ostream>
#include <set>
#include <span>
#include <string>
#include <string_view>
#include <vector>


namespace sql {
class ConnectionFactory;
}

namespace generator {
class GeneratedTable;

struct ForeignKeyMetadata {
  std::vector<std::string> columns;
  std::string referenced_table;
  std::vector<std::string> referenced_columns;
};

struct TableMetadata {
  std::vector<std::string> primary_key_columns;
  std::vector<ForeignKeyMetadata> foreign_keys;
};


class GeneratorState {
  friend struct Registrar;
  const sql::Engine engine_;
  const std::filesystem::path basePath_;
  const CloudProvider dataProvider_;
  const std::string dataPath_;
  const dbprove::StorageVariant storageVariant_;
  static constexpr std::string_view colSeparator_ = "|";
  static constexpr std::string_view rowSeparator_ = "\n";

  struct TransparentLess {
    using is_transparent = void;

    bool operator()(const std::string_view lhs, const std::string_view rhs) const {
      return lhs < rhs;
    }
  };


  std::set<std::string, TransparentLess> ready_tables_ = {};

public:
  explicit GeneratorState(const sql::Engine& engine, const std::filesystem::path& basePath,
                          CloudProvider dataProvider = CloudProvider::NONE, std::string dataPath = "",
                          dbprove::StorageVariant storageVariant = dbprove::StorageVariant::Native);
  ~GeneratorState();

  [[nodiscard]] const sql::Engine& engine() const { return engine_; }
  [[nodiscard]] CloudProvider cloudProvider() const { return dataProvider_; }
  [[nodiscard]] const std::string& dataPath() const { return dataPath_; }
  [[nodiscard]] dbprove::StorageVariant storageVariant() const { return storageVariant_; }

  /**
   * Makes sure that a table is availabe on the given connection
   * @param table_name To guarantee exists and is ready
   * @param conn Connection to generate the table at
   * @return Rowcount of the generated table
   */
  void ensure(std::string_view table_name, sql::ConnectionFactory& conn);

  /**
   * Makes sure that a table is available on the given connection
   * @param table_names To guarantee exists and is ready
   * @param conn Connection to generate the table at
   * @return Rowcount of the generated table
   */
  void ensure(std::span<const std::string_view> table_names, sql::ConnectionFactory& conn);

  /**
   * Makes sure an entire dataset is available on the given connection.
   * @param dataset_name Dataset registered through REGISTER_TABLE
   * @param conn Connection factory used for ensure/load operations
   */
  void ensureDataset(std::string_view dataset_name, sql::ConnectionFactory& conn);

  /**
   * Make sure local source files for an entire dataset have been generated or
   * downloaded and registered, without loading them into a database.
   * This is useful for engines that mount staged parquet directly at startup.
   */
  void ensureDatasetFiles(std::string_view dataset_name);

  /**
   * Make sure the local CSV/parquet source files for a table exist and return
   * the expected row count.
   */
  sql::RowCount generate(std::string_view table_name);
  /**
   * Load a table
   * @param table_name Table to load
   * @return Amount rows in the table
   */
  sql::RowCount load(std::string_view table_name, sql::ConnectionBase& conn);
  /**
   * Lets the state know that we've made the input files necessary for the generation
   * @param table_name The table we've generated
   * @param csv_paths The local CSV input files
   * @param parquet_paths The local parquet input files
   */
  void registerGeneration(std::string_view table_name,
                          std::vector<std::filesystem::path> csv_paths,
                          std::vector<std::filesystem::path> parquet_paths) const;

  /**
   * @brief Prints a summary of all tables that were ensured/generated during this run.
   * @param out The output stream to print to
   */
  void printSummary(std::ostream& out) const;

  GeneratedTable& table(std::string_view table_name) const;
  static bool contains(std::string_view table_name);
  static bool containsDataset(std::string_view dataset_name);
  [[nodiscard]] const std::filesystem::path& basePath() const { return basePath_; }

  static constexpr std::string_view columnSeparator() { return colSeparator_; }
  static constexpr std::string_view rowSeparator() { return rowSeparator_; }
};


struct Registrar {
  Registrar(std::string_view table_name,
            std::string_view dataset_name,
            std::string_view ddl,
            sql::RowCount rows,
            size_t expected_file_count,
            TableMetadata metadata = {});
};
}

/**
 * Macros to register table source functions.
 *
 *  Usage:
 *      REGISTER_TABLE("<name>", "<dataset>", <ddl>, <rows>, <fileCount>);
 *
 */

#define CONCATENATE_DETAIL(x, y) x##y
#define CONCATENATE(x, y) CONCATENATE_DETAIL(x, y)
#define LIST(...) { __VA_ARGS__ }
#define EXPAND(...) __VA_ARGS__
#define UNPARENTHESIS(x) EXPAND x            // x must be of the form ( ... )
#define BRACED(...) { __VA_ARGS__ }


#define REGISTER_TABLE(NAME, DATASET, DDL, ROWS, FILE_COUNT) \
    static inline generator::Registrar CONCATENATE(_registrar_, __COUNTER__)(NAME, DATASET, DDL, ROWS, FILE_COUNT);

#define REGISTER_TABLE_WITH_METADATA(NAME, DATASET, DDL, ROWS, FILE_COUNT, METADATA) \
    static inline generator::Registrar CONCATENATE(_registrar_, __COUNTER__)(NAME, DATASET, DDL, ROWS, FILE_COUNT, METADATA);
