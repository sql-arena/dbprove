#include "run_theorems.h"

#include "Messages.h"
#include "PreAmple.h"
#include "types.h"


struct TheoremState;

void run_theorems(std::string_view theorem_class,
                  const TheoremCommandMap& commandMap,
                  const std::vector<std::string>& theorems,
                  const TheoremState& state) {
  if (theorems.size() == 1 && theorems.back() == theorem_class) {
    for (const auto& [t, meta] : commandMap) {
      ux::PreAmpleTheorem(t);
      meta.func(std::string(t), state);
    }
  } else {
    for (const auto& t : theorems) {
      if (commandMap.contains(t)) {
        ux::PreAmpleTheorem(t);
        commandMap.at(t).func(t, state);
      } else {
        ux::Error("Unknown theorem: " + t);
      }
    }
  }
}