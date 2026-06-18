#pragma once
#include <dbprove/generator/generator_state.h>
#include <dbprove/common/storage_variant.h>
#include <dbprove/sql/explain/plan.h>
#include <vector>
#include <string>
#include <memory>
#include <functional>
#include <optional>
#include <ostream>
#include <filesystem>
#include <stdexcept>

#include "dbprove/sql/connection_factory.h"
#include "dbprove/sql/sql_type.h"
#include <magic_enum/magic_enum.hpp>

namespace dbprove::theorem {
namespace ee {
void prepareJoinScaleArtifacts(std::ostream& console, const std::optional<std::string>& source_parquet_dir);
}

class Proof;
class RunCtx;
class Theorem;
class Query;
class Data;
class DataExplain;
class DataQuery;
using TheoremFunction = std::function<void(Proof& state)>;

class DatasetBootstrapException : public std::runtime_error {
public:
  using std::runtime_error::runtime_error;
};

/**
* The type of Theorem
 */
enum class Category {
  CLI = 1,
  WLM = 2,
  PLAN = 3,
  EE = 4,
  SE = 5,
  TEST = 6,
  UNKNOWN = 0
};

inline std::string_view to_string(const Category type) { return magic_enum::enum_name(type); }


/**
 * Tag riding along on the Theorem
 */
class Tag {
public:
  explicit Tag(std::string name);

  bool operator==(const Tag& other) const { return name == other.name; }
  bool operator!=(const Tag& other) const { return !(*this == other); }
  bool operator<(const Tag& other) const { return name < other.name; }
  std::string name;
};

inline std::string_view to_string(const Tag& tag) { return tag.name; }

/**
 * Convert from a name to the enum
 * @param type_name Name to convert
 * @return Enum matching the name
 */
Category typeEnum(const std::string& type_name);

/**
 * All types by name
 * @return set of all types
 */
std::set<std::string_view> allTypeNames();

/**
 * Any datapoint that supports a Proof
 */
class Data {
public:
  virtual ~Data() = default;

  enum class Type {
    EXPLAIN,
    QUERY
  };

  const Type type;

  explicit Data(const Type type)
    : type(type) {
  }

  virtual void render(Proof& out) {
  }
};

/**
 * Explain plans and all the analysis that goes with it
 */
class DataExplain final : public Data {
public:
  explicit DataExplain(std::unique_ptr<sql::explain::Plan> plan)
    : Data(Type::EXPLAIN)
    , plan(std::move(plan)) {
  }

  std::unique_ptr<sql::explain::Plan> plan;

  void render(Proof& proof) override;
};


class DataQuery final : public Data {
public:
  explicit DataQuery(Query& query)
    : Data(Type::QUERY)
    , query(query) {
  }

  Query& query;
  void render(Proof& proof) override;
};

enum class Unit {
  Rows,
  COUNT,
  Microseconds,
  Query,
  Magnitude,
  Plan,
  Status,
  Text
};

constexpr inline std::string_view to_string(const Unit unit) { return magic_enum::enum_name(unit); }

struct RuntimeSummary {
  std::optional<int64_t> best_us;
  std::optional<int64_t> avg_us;
  std::optional<int64_t> min_us;
  std::optional<int64_t> max_us;
  std::optional<double> stddev_us;
};

struct QueryProofData {
  std::optional<std::string> sql;
  std::optional<std::string> start_time;
  std::optional<std::string> status;
  std::optional<std::string> error_message;
  std::optional<std::string> plan;
  std::optional<int64_t> time_us;
  std::map<std::string, int64_t> operator_rows;
  std::map<std::string, std::map<std::string, int64_t>> mis_estimates;
};

/**
 * A Proof is the holder of all data that is the result of proving a theorem
 */
class Proof {
public:
  Proof(const Theorem& theorem, RunCtx& parent)
    : theorem(theorem)
    , state(parent) {
  }

  ~Proof();
  const Theorem& theorem;
  std::vector<std::unique_ptr<Data>> data;
  [[nodiscard]] sql::ConnectionFactory& factory() const;
  Proof& ensure(const std::string& table);
  Proof& ensureDataset(const std::string& dataset);
  /**
   * Make sure the schema exists
   * @param schema To create if not there
   * @return
   */
  Proof& ensureSchema(const std::string& schema);
  void render();
  [[nodiscard]] std::ostream& console() const;
  [[nodiscard]] bool artifactMode() const;
  [[nodiscard]] std::optional<uint32_t> queryTimeoutSeconds() const;
  [[nodiscard]] size_t timingRuns() const;
  [[nodiscard]] const std::optional<std::string>& parquetDir() const;
  QueryProofData& beginQuery(std::string sql);
  QueryProofData& ensureQuery();
  void setCurrentQueryStartTime(std::string start_time);
  void setCurrentQueryPlan(std::string plan);
  void setCurrentQueryBestRuntimeMicroseconds(int64_t time_us);
  void setRuntimeSummaryMicroseconds(int64_t best_us, int64_t avg_us, int64_t min_us, int64_t max_us,
                                     double stddev_us);
  void setCurrentQueryOperatorRows(const std::string& operation, int64_t rows);
  void setCurrentQueryMisEstimate(const std::string& operation, const std::string& magnitude, int64_t count);
  void setRunStatus(std::string status);
  void setErrorMessage(std::string error_message);
  [[nodiscard]] std::string toJson() const;

private:
  RunCtx& state;
  bool rendered_ = false;
  std::vector<QueryProofData> queries_;
  std::optional<size_t> current_query_index_;
  RuntimeSummary runtime_summary_;
  std::optional<std::string> run_status_;
  std::optional<std::string> error_message_;
};


class Theorem {
  std::set<Tag> tags_ = {};
  std::set<Category> categories_ = {};
  std::string tags_string_;
  std::string categories_string_;
  std::optional<dbprove::StorageVariant> required_storage_variant_;

public:
  Theorem(std::string theorem, std::string description, const TheoremFunction& func,
          std::optional<sql::RowCount> expected_row_count = std::nullopt,
          std::optional<std::string> display_name = std::nullopt)
    : name(std::move(theorem))
    , display_name_(display_name.has_value() ? std::move(*display_name) : name)
    , description(std::move(description))
    , expected_row_count_(expected_row_count)
    , func(func) {
  }

  bool operator<(const Theorem& other) const {
    return name < other.name;
  }

  bool operator==(const Theorem& other) const {
    return name == other.name;
  }

  bool operator!=(const Theorem& other) const {
    return !(*this == other);
  }

  void addTag(const Tag& tag);
  bool hasTag(const Tag& tag) const;
  void addCategory(Category category);

  std::string tags_to_string() const {
    return tags_string_;
  }

  std::string categories_to_string() const {
    return categories_string_;
  }

  [[nodiscard]] const std::optional<sql::RowCount>& expectedRowCount() const {
    return expected_row_count_;
  }

  void requireStorageVariant(const dbprove::StorageVariant variant) {
    required_storage_variant_ = variant;
  }

  [[nodiscard]] std::optional<dbprove::StorageVariant> requiredStorageVariant() const {
    return required_storage_variant_;
  }

  [[nodiscard]] const std::string& displayName() const {
    return display_name_;
  }

  std::string name;
  std::string description;
  TheoremFunction func;

private:
  std::string display_name_;
  std::optional<sql::RowCount> expected_row_count_;
};


/**
 * RunContext must be passed into the Theorem prove function to provide the environment that is used for proving
 * It is also where the proofs are kept for returning to caller
 */
class RunCtx {
private:
  std::filesystem::path proof_directory_path_;

public:
  const sql::Engine& engine;
  std::string engine_version;
  dbprove::StorageVariant storage_variant;
  const sql::Credential& credentials;
  generator::GeneratorState& generator;
  sql::ConnectionFactory factory;
  std::ostream& console;
  bool artifact_mode = false;
  std::optional<uint32_t> query_timeout_seconds;
  size_t timing_runs = 3;
  std::optional<std::string> parquet_dir;
  std::set<std::string> ensured_datasets;
  std::vector<std::unique_ptr<Proof>> proofs;
  void writeProofJson(std::string_view proof_name, std::string_view content) const;
  RunCtx(const sql::Engine& engine, const sql::Credential& credentials, generator::GeneratorState& generator,
         std::ostream& console, std::string engine_version, std::optional<std::string> connection_artifacts_path = std::nullopt,
         dbprove::StorageVariant storage_variant = dbprove::StorageVariant::Native,
         std::optional<uint32_t> query_timeout_seconds = std::nullopt,
         size_t timing_runs = 3,
         std::optional<std::string> parquet_dir = std::nullopt,
         std::optional<std::filesystem::path> proof_directory = std::nullopt,
         bool artifact_mode = false);

  ~RunCtx();
};


/**
 * Call this before using the library
 */
void init();
/**
 * Parse a list of theorems and turn them into properly typed theorems
 * @param theorems List of strings to parse
 * @return The Theorems to run based on the input
 */
std::vector<const Theorem*> parse(const std::vector<std::string>& theorems);


/**
 * Run all theorems provided.
 * @param theorems Theorems per parse call
 * @param input_state Caller supplied state with all the info needed to run
 */
bool prove(const std::vector<const Theorem*>& theorems, RunCtx& input_state);
}
