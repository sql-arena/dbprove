#pragma once
#include <map>
#include "sql/Credential.h"
#include <functional>

#include "generator/generator_state.h"
#include "sql/connection_factory.h"

struct TheoremData {
  enum class Type {
    EXPLAIN
  };

  const Type type;

  explicit TheoremData(const Type type)
    : type(type) {
  }
};

struct TheoremDataExplain : public TheoremData {
  explicit TheoremDataExplain(std::unique_ptr<sql::explain::Plan> plan)
    : TheoremData(TheoremData::Type::EXPLAIN)
    , plan(std::move(plan)) {
  }
  std::unique_ptr<sql::explain::Plan> plan;
};

struct TheoremState;

struct TheoremProof {
  TheoremProof(const std::string& theorem, TheoremState& parent ): theorem(theorem), state(parent) {}
  TheoremProof(const std::string_view theorem, TheoremState& parent ): theorem(std::string(theorem)), state(parent) {}
  const std::string theorem;
  std::vector<TheoremData> data;
  sql::ConnectionFactory& factory() const;
  TheoremProof& ensure(const std::string& table);
private:
  TheoremState& state;
};

struct TheoremState {
  const sql::Engine& engine;
  const sql::Credential& credentials;
  generator::GeneratorState& generator;
  sql::ConnectionFactory factory{engine, credentials};
  std::vector<TheoremProof> proofs;
};

using TheoremFunction = std::function<void(TheoremProof& state)>;
using TheoremMetadata = struct {
  std::string_view description;
  TheoremFunction func;
};
using TheoremCommandMap = std::map<std::string_view, TheoremMetadata>;
