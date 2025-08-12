#pragma once
#include "types.h"


void run_theorems(std::string_view theorem_class,
                  const TheoremCommandMap& commandMap,
                  const std::vector<std::string> &theorems,
                  const TheoremState& state);
