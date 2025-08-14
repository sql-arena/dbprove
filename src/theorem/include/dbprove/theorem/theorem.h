#pragma once
#include "theorem_type.h"

namespace dbprove::theorem {
  void prove(const std::vector<std::string>& theorems, TheoremState& input_state);
}