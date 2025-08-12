#pragma once
#include <string>
#include <vector>

struct TheoremState;

namespace plan {
    void prove(const std::vector<std::string>& theorems, const TheoremState& state);
}
