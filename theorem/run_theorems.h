#pragma once
#include "Messages.h"


void run_theorems(std::string_view theorem_class,
                  const TheoremCommandMap& commandMap,
                  const std::vector<std::string> &theorems,
                  sql::Engine engine,
                  const sql::Credential &credentials) {
  if (theorems.size() == 1 && theorems.back() == theorem_class) {
    for (const auto& [t, meta] : commandMap) {
      ux::PreAmpleTheorem(t);
      meta.func(std::string(t), engine, credentials);
    }
  }
  else {
    for (const auto &t: theorems) {
      if (commandMap.contains(t)) {
        ux::PreAmpleTheorem(t);
        commandMap.at(t).func(t, engine, credentials);
      } else {
        ux::Error("Unknown theorem: " + t);
      }
    }
  }
}