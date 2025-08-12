#pragma once
#include <map>
#include "sql/Credential.h"
#include <functional>

#include "generator/generator_state.h"


struct TheoremState {
  const sql::Engine& engine;
  const sql::Credential& credentials;
  generator::GeneratorState& generator;
};

using TheoremFunction = std::function<void(const std::string& theorem, const TheoremState& state)>;
using TheoremMetadata = struct {
  std::string_view description;
  TheoremFunction func;
};
using TheoremCommandMap = std::map<std::string_view, TheoremMetadata>;


