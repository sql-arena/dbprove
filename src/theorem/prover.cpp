#include "theorem.h"
#include <string>
#include <vector>
#include <ranges>
#include <dbprove/ux/ux.h>

#include "init.h"

namespace dbprove::theorem {
void run_theorem(const Theorem& theorem,
                 RunState& state) {
  state.proofs.push_back(std::make_unique<Proof>(theorem, state));
  theorem.func(*state.proofs.back());
}

void prove(const std::vector<const Theorem*>& theorems, RunState& input_state) {
  auto prev_type = Type::UNKNOWN;
  for (const auto& theorem : theorems) {
    run_theorem(*theorem, input_state);
    if (theorem->type != prev_type) {
      ux::PreAmple("NEW category");
    }
    prev_type = theorem->type;
  }
}

std::vector<const Theorem*> parse(const std::vector<std::string>& theorems) {
  std::vector<const Theorem*> parsed_theorems;
  if (theorems.size() == 0) {
    // If user did not supply theorems, default to all
    for (auto& t : std::views::values(allTheorems())) {
      parsed_theorems.push_back(&t);
    }
    return parsed_theorems;
  }

  for (const auto& t : theorems) {
    // TODO: Handle the case where only the prefix is passed
    if (allTheorems().contains(t)) {
      std::cerr << "Unknown theorem type: " << t << ".";
      std::cerr << "the following prefixes are supported: ";
      //      std::cerr << join(theorem_names, ",");
      //      for (const auto& pair : theorem_types) {
      //        std::cerr << pair.first << ", ";
      //      }
      std::cerr << std::endl;
      std::exit(1);
    }
    parsed_theorems.push_back(&allTheorems().at(t));
  }
  return parsed_theorems;
}
}