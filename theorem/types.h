#pragma once
#include <map>
#include "sql/Credential.h"
#include <functional>

using TheoremFunction = std::function<void(const std::string& theorem, sql::Engine&, const sql::Credential&)>;
using TheoremMetadata = struct {
  std::string_view description;
  TheoremFunction func;
};
using TheoremCommandMap = std::map<std::string_view, TheoremMetadata>;