#include <plog/Log.h>
#include "theorem.h"
#include "dbprove/sql/sql_exceptions.h"
#include <dbprove/sql/sql.h>
#include <dbprove/common/file_utility.h>
#include <fstream>
#include <filesystem>

namespace dbprove::theorem {
Proof::~Proof() = default;

sql::ConnectionFactory& Proof::factory() const {
  return state.factory;
}

Proof& Proof::ensure(const std::string& table) {
  if (state.artifact_mode) {
    PLOGD << "Artifact mode: skipping ensure('" << table << "')";
    return *this;
  }
  state.generator.ensure(table, factory());
  return *this;
}

Proof& Proof::ensureDataset(const std::string& dataset) {
  if (state.artifact_mode) {
    PLOGI << "Artifact mode: skipping dataset ensure/tuning for '" << dataset << "'";
    return *this;
  }
  if (!state.ensured_datasets.insert(dataset).second) {
    PLOGD << "Dataset '" << dataset << "' already ensured in this run; skipping ensure, summary, and tuning.";
    return *this;
  }

  state.generator.ensureDataset(dataset, factory());
  state.generator.printSummary(state.console);

  const auto project_root = dbprove::common::get_project_root();
  const auto tune_file_path = project_root / "src" / "sql" / state.engine.internalName() / "tune" / (dataset + ".sql");
  if (std::filesystem::exists(tune_file_path)) {
    PLOGI << "Tuning dataset '" << dataset << "' with " << tune_file_path.string();
    std::ifstream ifs(tune_file_path);
    if (!ifs.is_open()) {
      PLOGW << "Failed to open " << tune_file_path.string();
      return *this;
    }

    const std::string sql((std::istreambuf_iterator<char>(ifs)), std::istreambuf_iterator<char>());
    auto conn = state.factory.create();
    conn->execute(sql);
    PLOGI << "Dataset tuning complete for '" << dataset << "'";
  }

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
  for (const auto& d : data) {
    d->render(*this);
  }
}

std::ostream& Proof::console() const {
  return state.console;
}


void Proof::writeCsv(const std::string& key, std::string value, const Unit unit) const {
  static std::atomic<uint64_t> counter{1};

  state.writeCsv(std::vector<std::string_view>{state.engine.name(),
                                               std::to_string(++counter),
                                               theorem.categories_to_string(),
                                               theorem.tags_to_string(),
                                               theorem.name,
                                               theorem.description,
                                               key,
                                               value,
                                               to_string(unit)});
}

std::ostream& Proof::csv() const {
  return state.csv;
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

bool Theorem::hasTag(const Tag& tag) const {
  return tags_.contains(tag);
}

void Theorem::addCategory(const Category category) {
  categories_.insert(category);
  categories_string_ = sorted_join(categories_);
}
}
