#include "run_theorems.h"

#include "ux/Messages.h"
#include "ux/PreAmple.h"
#include "types.h"


struct TheoremProof;

void run_theorems(std::string_view theorem_class,
                  const TheoremCommandMap& commandMap,
                  const std::vector<std::string>& theorems,
                  TheoremState& state) {
  if (theorems.size() == 1 && theorems.back() == theorem_class) {
    for (const auto& [t, meta] : commandMap) {
      ux::PreAmpleTheorem(t);
      auto p = TheoremProof(t, state);
      meta.func(p);
    }
  } else {
    for (const auto& t : theorems) {
      if (commandMap.contains(t)) {
        ux::PreAmpleTheorem(t);
        auto p = TheoremProof(t, state);
        commandMap.at(t).func(p);
      } else {
        ux::Error("Unknown theorem: " + t);
      }
    }
  }
}