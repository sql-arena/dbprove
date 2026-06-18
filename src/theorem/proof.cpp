#include <cmath>
#include <filesystem>
#include <fstream>

#include "dbprove/sql/sql_exceptions.h"
#include "theorem.h"
#include <dbprove/common/file_utility.h>
#include <dbprove/sql/sql.h>
#include <nlohmann/json.hpp>
#include <plog/Log.h>

namespace dbprove::theorem {
namespace {
std::vector<std::string> splitCsvLikeList(const std::string& value) {
  std::vector<std::string> parts;
  if (value.empty()) {
    return parts;
  }

  std::string current;
  for (const char c : value) {
    if (c == ',') {
      if (!current.empty()) {
        parts.push_back(current);
        current.clear();
      }
      continue;
    }
    current.push_back(c);
  }
  if (!current.empty()) {
    parts.push_back(current);
  }
  return parts;
}

double roundToThreeDecimals(const double value) {
  return std::round(value * 1000.0) / 1000.0;
}

double microsecondsToRoundedMilliseconds(const int64_t microseconds) {
  return roundToThreeDecimals(static_cast<double>(microseconds) / 1000.0);
}
}  // namespace

Proof::~Proof() = default;

sql::ConnectionFactory& Proof::factory() const { return state.factory; }

Proof& Proof::ensureDataset(const std::string& dataset) {
  if (state.artifact_mode) {
    PLOGI << "Artifact mode: skipping dataset ensure/tuning for '" << dataset << "'";
    return *this;
  }
  if (state.ensured_datasets.contains(dataset)) {
    PLOGD << "Dataset '" << dataset << "' already ensured in this run; skipping ensure, summary, and tuning.";
    return *this;
  }

  try {
    auto conn = state.factory.create();
    state.generator.ensureDataset(dataset, factory());
    state.generator.printSummary(state.console);

    const auto project_root = dbprove::common::get_project_root();
    const auto tune_file_path = project_root / "src" / "sql" / state.engine.internalName() / "tune" / (dataset + ".sql");
    if (std::filesystem::exists(tune_file_path)) {
      if (conn->shouldSkipDatasetTuning(dataset)) {
        PLOGI << "Skipping dataset tuning for '" << dataset
              << "' because the engine reported the dataset is already tuned.";
        return *this;
      }

      PLOGI << "Tuning dataset '" << dataset << "' with " << tune_file_path.string();
      std::ifstream ifs(tune_file_path);
      if (!ifs.is_open()) {
        PLOGW << "Failed to open " << tune_file_path.string();
        return *this;
      }

      const std::string sql((std::istreambuf_iterator<char>(ifs)), std::istreambuf_iterator<char>());
      conn->execute(sql);
      PLOGI << "Dataset tuning complete for '" << dataset << "'";
    }
  } catch (const std::exception& e) {
    throw DatasetBootstrapException("Dataset '" + dataset + "' bootstrap failed: " + e.what());
  }

  state.ensured_datasets.insert(dataset);
  return *this;
}

Proof& Proof::ensureSchema(const std::string& schema) {
  try {
    state.factory.create()->createSchema(schema);
  } catch (sql::Exception& e) {
    PLOGD << "Schema creation failed (might already exist): " << e.what();
  }
  return *this;
}

void Proof::render() {
  if (rendered_) {
    return;
  }
  for (const auto& d : data) {
    d->render(*this);
  }
  rendered_ = true;
}

std::ostream& Proof::console() const { return state.console; }

bool Proof::artifactMode() const { return state.artifact_mode; }

std::optional<uint32_t> Proof::queryTimeoutSeconds() const { return state.query_timeout_seconds; }

size_t Proof::timingRuns() const { return state.timing_runs; }

const std::optional<std::string>& Proof::parquetDir() const { return state.parquet_dir; }

QueryProofData& Proof::beginQuery(std::string sql) {
  queries_.push_back(QueryProofData{.sql = std::move(sql)});
  current_query_index_ = queries_.size() - 1;
  return queries_.back();
}

QueryProofData& Proof::ensureQuery() {
  if (!current_query_index_.has_value()) {
    return beginQuery(std::string());
  }
  return queries_[*current_query_index_];
}

void Proof::setCurrentQueryStartTime(std::string start_time) {
  ensureQuery().start_time = std::move(start_time);
}

void Proof::setCurrentQueryPlan(std::string plan) {
  ensureQuery().plan = std::move(plan);
}

void Proof::setCurrentQueryBestRuntimeMicroseconds(const int64_t time_us) {
  ensureQuery().time_us = time_us;
}

void Proof::setRuntimeSummaryMicroseconds(const int64_t best_us, const int64_t avg_us, const int64_t min_us,
                                          const int64_t max_us, const double stddev_us) {
  runtime_summary_.best_us = best_us;
  runtime_summary_.avg_us = avg_us;
  runtime_summary_.min_us = min_us;
  runtime_summary_.max_us = max_us;
  runtime_summary_.stddev_us = stddev_us;
}

void Proof::setCurrentQueryOperatorRows(const std::string& operation, const int64_t rows) {
  ensureQuery().operator_rows[operation] = rows;
}

void Proof::setCurrentQueryMisEstimate(const std::string& operation, const std::string& magnitude, const int64_t count) {
  ensureQuery().mis_estimates[operation][magnitude] = count;
}

void Proof::setRunStatus(std::string status) {
  run_status_ = std::move(status);
}

void Proof::setErrorMessage(std::string error_message) {
  error_message_ = std::move(error_message);
}

std::string Proof::toJson() const {
  nlohmann::json document = nlohmann::json::object();
  document["theorem"] = nlohmann::json::object();
  document["theorem"]["name"] = theorem.name;
  document["theorem"]["displayName"] = theorem.displayName();
  document["theorem"]["description"] = theorem.description;
  document["theorem"]["categories"] = splitCsvLikeList(theorem.categories_to_string());
  document["theorem"]["tags"] = splitCsvLikeList(theorem.tags_to_string());
  document["engine"] = state.engine.name();
  document["version"] = state.engine_version;
  document["storageVariant"] = to_string(state.storage_variant);
  document["queries"] = nlohmann::json::array();

  if (runtime_summary_.best_us.has_value() || runtime_summary_.avg_us.has_value() || runtime_summary_.min_us.has_value() ||
      runtime_summary_.max_us.has_value() || runtime_summary_.stddev_us.has_value()) {
    document["runtime"] = nlohmann::json::object();
    if (runtime_summary_.avg_us.has_value()) {
      document["runtime"]["avgMs"] = microsecondsToRoundedMilliseconds(*runtime_summary_.avg_us);
    }
    if (runtime_summary_.best_us.has_value()) {
      document["runtime"]["bestMs"] = microsecondsToRoundedMilliseconds(*runtime_summary_.best_us);
    }
    if (runtime_summary_.max_us.has_value()) {
      document["runtime"]["maxMs"] = microsecondsToRoundedMilliseconds(*runtime_summary_.max_us);
    }
    if (runtime_summary_.min_us.has_value()) {
      document["runtime"]["minMs"] = microsecondsToRoundedMilliseconds(*runtime_summary_.min_us);
    }
    if (runtime_summary_.stddev_us.has_value()) {
      document["runtime"]["stdDevMs"] = microsecondsToRoundedMilliseconds(
          static_cast<int64_t>(std::llround(*runtime_summary_.stddev_us)));
    }
  }

  for (size_t i = 0; i < queries_.size(); ++i) {
    const auto& query_data = queries_[i];
    nlohmann::json query_document = nlohmann::json::object();
    if (query_data.sql.has_value() && !query_data.sql->empty()) {
      query_document["sql"] = *query_data.sql;
    }
    if (query_data.start_time.has_value()) {
      query_document["startTime"] = *query_data.start_time;
    }
    if (query_data.plan.has_value()) {
      query_document["plan"] = *query_data.plan;
    }
    if (query_data.time_us.has_value()) {
      query_document["timeMs"] = microsecondsToRoundedMilliseconds(*query_data.time_us);
    }
    if (!query_data.operator_rows.empty()) {
      query_document["operatorRows"] = query_data.operator_rows;
    }
    if (!query_data.mis_estimates.empty()) {
      query_document["misEstimates"] = query_data.mis_estimates;
    }

    if (query_data.status.has_value()) {
      query_document["status"] = *query_data.status;
    } else if (run_status_.value_or("OK") == "OK") {
      query_document["status"] = "OK";
    } else {
      query_document["status"] = (i + 1 == queries_.size()) ? *run_status_ : "OK";
    }

    if (query_data.error_message.has_value()) {
      query_document["errorMessage"] = *query_data.error_message;
    } else if (error_message_.has_value() && i + 1 == queries_.size() && run_status_.value_or("OK") != "OK") {
      query_document["errorMessage"] = *error_message_;
    }

    document["queries"].push_back(std::move(query_document));
  }

  return document.dump(2);
}

template <typename Iterable>
std::string sorted_join(const Iterable& items, const std::string& delimiter = ",") {
  std::vector<typename Iterable::value_type> sorted_items(items.begin(), items.end());
  std::sort(sorted_items.begin(), sorted_items.end());
  std::ostringstream oss;
  for (size_t i = 0; i < sorted_items.size(); ++i) {
    oss << to_string(sorted_items[i]);
    if (i + 1 < sorted_items.size())
      oss << delimiter;
  }
  return oss.str();
}

void Theorem::addTag(const Tag& tag) {
  tags_.insert(tag);
  tags_string_ = sorted_join(tags_);
}

bool Theorem::hasTag(const Tag& tag) const { return tags_.contains(tag); }

void Theorem::addCategory(const Category category) {
  categories_.insert(category);
  categories_string_ = sorted_join(categories_);
}
}  // namespace dbprove::theorem
