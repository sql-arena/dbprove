#pragma once
#include <string>
#include <vector>

struct TheoremState;

namespace plan {
    void prove(const std::vector<std::string>& theorems, TheoremState& input_state);
}
