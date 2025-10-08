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
  PLOGI << "Reading Version...";
  const std::string version = input_state.factory.create()->version();
  input_state.writeCsv(std::vector<std::string_view>{input_state.engine.name(),
                                                     "0",
                                                     "CONFIG",
                                                     "CONFIG-VERSION",
                                                     "Version of Engine",
                                                     "version",
                                                     version,
                                                     "version"});
  PLOGI << "The Version of the engine is: " << version;
}

void prove(const std::vector<const Theorem*>& theorems, RunCtx& input_state) {
  auto prev_type = Type::UNKNOWN;

  writeVersion(input_state);

  for (const auto& theorem : theorems) {
    if (theorem->type != prev_type) {
      ux::PreAmple(input_state.console, to_string(theorem->type));
    }
    ux::PreAmpleTheorem(input_state.console, theorem->name);
    run_theorem(*theorem, input_state);
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
        /* User passed a category: pick everything.*/
        auto all_theorems_in_type = allTheoremsInType(typeEnum(t));
        parsed_theorems.insert(all_theorems_in_type.begin(), all_theorems_in_type.end());
        continue;
      }
      if (!allTheorems().contains(t)) {
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