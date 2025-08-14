#include "types.h"

#include "sql/sql_exceptions.h"

std::string TheoremDataExplain::render() {
  return plan->render();
}

sql::ConnectionFactory& TheoremProof::factory() const {
  return state.factory;
}

TheoremProof& TheoremProof::ensure(const std::string& table) {
  state.generator.ensure(table, factory());
  return *this;
}

TheoremProof& TheoremProof::ensureSchema(const std::string& schema) {
  try {
    state.factory.create()->execute("CREATE SCHEMA " + schema);
  }
  catch (sql::Exception&) {
    // NOOP
  }
  return *this;
}

std::ostream& TheoremProof::console() const {
  return state.console;
}

std::ostream& TheoremProof::csv() const {
  return state.csv;
}