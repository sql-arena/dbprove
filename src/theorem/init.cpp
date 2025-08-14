#include "init.h"
#include "theorem.h"
#include "plan/prover.h"
#include "cli/prover.h"

namespace dbprove::theorem {

  TheoremMap all_theorems_;

  void init() {
    plan::init();
    cli::init();
  }

  void addTheorem(Type type, std::string name, std::string description, const TheoremFunction& func) {
    all_theorems_.emplace(name, Theorem{type, name, description, func});
  }

  const TheoremMap& allTheorems() {
    return all_theorems_;
  }
}