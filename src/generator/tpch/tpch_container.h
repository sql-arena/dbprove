#pragma once
#include <string>

#include "generator/variable_integer.h"

namespace generator {
/// @brief Generates type strings as per TPC-H spec section 4.2.2.13
class TpchContainer {
  VariableInteger random_source_;

public:
  TpchContainer() = default;
  std::string next();
};
} // namespace generator