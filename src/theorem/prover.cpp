#include "theorem.h"
#include <string>
#include <vector>
#include <ranges>
#include <dbprove/ux/ux.h>

#include "init.h"
#include <plog/Log.h>

namespace dbprove::theorem {
void run_theorem(const Theorem& theorem, RunCtx& state) {
  state.proofs.push_back(std::make_unique<Proof>(theorem, state));
  theorem.func(*state.proofs.back());
}

void writeVersion(RunCtx& input_state) {
  if (input_state.artifact_mode) {
    PLOGI << "Artifact mode: skipping engine version lookup";
    return;
  }
  PLOGI << "Reading Version...";
  const std::string version = input_state.factory.create()->version();

  input_state.writeCsv(std::vector<std::string_view>{input_state.engine.name(),
                                                     "0",
                                                     allCategoriesAsString(),
                                                     "CONFIG",
                                                     "CONFIG-VERSION",
                                                     "Version of Engine",
                                                     "version",
                                                     version,
                                                     "version"});
  PLOGI << "The Version of the engine is: " << version;
}

void prove(const std::vector<const Theorem*>& theorems, RunCtx& input_state) {
  writeVersion(input_state);

  for (const auto& theorem : theorems) {
    ux::PreAmpleTheorem(input_state.console, theorem->name);
    run_theorem(*theorem, input_state);
  }
}

std::vector<const Theorem*> parse(const std::vector<std::string>& theorems) {
  // We only want to run each theorem once. Remove duplicates first.
  std::set<const Theorem*> parsed_theorems;
  if (theorems.size() == 0) {
    // If the user didn't supply any theorems, default to all
    for (auto& t : std::views::values(allTheorems())) {
      parsed_theorems.insert(t.get());
    }
  } else {
    for (const auto& t : theorems) {
      if (allTypeNames().contains(t)) {
        /* User passed a category: pick everything.*/
        auto all_theorems_in_type = allTheoremsInCategory(typeEnum(t));
        parsed_theorems.insert(all_theorems_in_type.begin(), all_theorems_in_type.end());
        continue;
      }
      
      bool found_by_tag = false;
      try {
        const Tag tag(t);
        for (const auto& theorem_ptr : std::views::values(allTheorems())) {
          if (theorem_ptr->hasTag(tag)) {
            parsed_theorems.insert(theorem_ptr.get());
            found_by_tag = true;
          }
        }
      } catch (...) {
        // Tag construction might fail if it's not uppercase or has invalid chars,
        // which is fine, we'll try it as a theorem name next.
      }

      if (found_by_tag) {
        continue;
      }

      if (!allTheorems().contains(t)) {
        /* Specific theorem*/
        throw std::runtime_error("Unknown theorem: " + t);
      }
      parsed_theorems.insert(allTheorems().at(t).get());
    }
  }
  // Run theorems in name order.
  std::vector sorted_theorems(parsed_theorems.begin(), parsed_theorems.end());
  std::ranges::sort(sorted_theorems, [](const Theorem* a, const Theorem* b) { return *a < *b; });
  return sorted_theorems;
}
}
