#include "init.h"

#include <ranges>

#include "theorem.h"
#include "plan/prover.h"
#include "cli/prover.h"

namespace dbprove::theorem::test { void init(); }

namespace dbprove::theorem {
TheoremMap theorem_map_;
CategoryMap theorem_category_map_;
CategorySet all_categories_ = {};
std::string all_categories_string_;

void init() {
  plan::init();
  cli::init();
  test::init();
}

Theorem& addTheorem(std::string name, std::string description, const TheoremFunction& func) {
  if (theorem_map_.contains(name)) {
    throw std::runtime_error("Theorem " + name + " already exists");
  }
  theorem_map_.emplace(name, std::make_unique<Theorem>(name, description, func));
  return *theorem_map_.at(name).get();
}

void stringifyCategories() {
  all_categories_string_ = "";
  for (const auto c : all_categories_) {
    all_categories_string_.append(to_string(c));
    if (c != *all_categories_.rbegin()) {
      all_categories_string_.append(", ");
    }
  }
}

void categoriseTheorem(Theorem& theorem, const Category category) {
  if (!theorem_category_map_.contains(category)) {
    theorem_category_map_.emplace(category, std::set<const Theorem*>());
  }
  theorem_category_map_[category].insert(&theorem);
  theorem.addCategory(category);
  auto [it, inserted] = all_categories_.insert(category);
  if (inserted) {
    stringifyCategories();
  }
}

void tagTheorem(Theorem& theorem, const Tag& tag) {
  theorem.addTag(tag);
}

const TheoremMap& allTheorems() {
  return theorem_map_;
}

const std::set<const Theorem*>& allTheoremsInCategory(const Category type) {
  return theorem_category_map_[type];
}

const CategorySet& allCategories() {
  return all_categories_;
}

std::string_view allCategoriesAsString() {
  return all_categories_string_;
}
}