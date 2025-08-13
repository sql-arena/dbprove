#include "types.h"

sql::ConnectionFactory& TheoremProof::factory() const {
  return state.factory;
}

TheoremProof& TheoremProof::ensure(const std::string& table) {
  state.generator.ensure(table, factory());
  return *this;
}