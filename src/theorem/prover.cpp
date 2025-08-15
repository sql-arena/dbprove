#include "theorem.h"
#include <string>
#include <vector>
#include <ranges>
#include <dbprove/ux/ux.h>

#include "init.h"

namespace dbprove::theorem {
void run_theorem(const Theorem& theorem,
                 RunCtx& state) {
  state.proofs.push_back(std::make_unique<Proof>(theorem, state));
  theorem.func(*state.proofs.back());
}

void prove(const std::vector<const Theorem*>& theorems, RunCtx& input_state) {
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
  std::set<const Theorem*> parsed_theorems;
  if (theorems.size() == 0) {
    // If user did not supply theorems, default to all
    for (auto& t : std::views::values(allTheorems())) {
      parsed_theorems.insert(t.get());
    }
  } else {
    for (const auto& t : theorems) {
      if (allTypeNames().contains(t)) {
        /* User passed a category, pick everything */
        auto all_theorems_in_type = allTheoremsInType(typeEnum(t));
        parsed_theorems.insert(all_theorems_in_type.begin(), all_theorems_in_type.end());
        continue;
      }
      if (allTheorems().contains(t)) {
        /* Specific theorem*/
        throw std::runtime_error("Unknown theorem: " + t);
      }
      parsed_theorems.insert(allTheorems().at(t).get());
    }
  }
  /* We want to process each category fully before moving on to the next */
  std::vector sorted_theorems(parsed_theorems.begin(), parsed_theorems.end());
  std::ranges::sort(sorted_theorems, [](const Theorem* a, const Theorem* b) {
    if (a->type != b->type) {
      return a->type < b->type;
    }
    return a->name < b->name;
  });

  return sorted_theorems;
}
}