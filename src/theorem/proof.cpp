#include "theorem.h"
#include "dbprove/sql/sql_exceptions.h"
#include <dbprove/sql/sql.h>

namespace dbprove::theorem {
Proof::~Proof() = default;

sql::ConnectionFactory& Proof::factory() const {
  return state.factory;
}

Proof& Proof::ensure(const std::string& table) {
  state.generator.ensure(table, factory());
  return *this;
}

Proof& Proof::ensureSchema(const std::string& schema) {
  try {
    state.factory.create()->createSchema(schema);
  } catch (sql::Exception&) {
    // NOOP
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

void Theorem::addCategory(const Category category) {
  categories_.insert(category);
  categories_string_ = sorted_join(categories_);
}
}