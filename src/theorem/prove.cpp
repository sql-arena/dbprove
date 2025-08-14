#include "theorem.h"
#include "types.h"
#include <string>
#include <vector>

void prove(const std::vector<std::string>& theorems, TheoremState& input_state) {
  switch (type) {
    case TheoremType::CLI:
      cli::prove(theorems, input_state);
    break;
    case TheoremType::PLAN:
      plan::prove(theorems, input_state);
    break;
  }
}