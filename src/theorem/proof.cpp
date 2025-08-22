#include "theorem.h"
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
    state.factory.create()->execute("CREATE SCHEMA " + schema);
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
  state.writeCsv(std::vector<std::string_view>{
      state.engine.name(),
      std::to_string(++counter),
      to_string(theorem.type),
      theorem.name,
      theorem.description,
      key,
      value,
      to_string(unit)});
}

std::ostream& Proof::csv() const {
  return state.csv;
}
}