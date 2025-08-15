#include "init.h"
#include "theorem.h"
#include "plan/prover.h"
#include "cli/prover.h"
#include <utility>
#include <set>

namespace dbprove::theorem {

  TheoremMap all_theorems_;
  std::map<Type, std::set<const Theorem*>> theorem_type_map_;

  void init() {
    plan::init();
    cli::init();
  }

  void addTheorem(Type type, std::string name, std::string description, const TheoremFunction& func) {
    if (all_theorems_.contains(name)) {
      throw std::runtime_error("Theorem " + name + " already exists");
    }
    all_theorems_[name] = std::make_unique<Theorem>(type, name, std::move(description), func);
    theorem_type_map_[type].insert(all_theorems_[name].get());
  }

  const TheoremMap& allTheorems() {
    return all_theorems_;
  }

  const std::set<const Theorem*>& allTheoremsInType(Type type) {
    return theorem_type_map_[type];
  }
}