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

std::ostream& Proof::console() const {
  return state.console;
}

std::ostream& Proof::csv() const {
  return state.csv;
}
}