#pragma once
#include <string>

#include "generator/variable_integer.h"

namespace generator {
/// @brief Generates type strings as per TPC-H spec section 4.2.2.13
class TpchTypes {
  VariableInteger random_source_;

public:
  TpchTypes() = default;
  std::string next();
};
}