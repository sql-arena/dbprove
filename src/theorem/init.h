#pragma once
#include "theorem.h"
#include <map>
#include <functional>

namespace dbprove::theorem {
using TheoremMap = std::map<std::string, std::unique_ptr<Theorem>>;
using CategoryMap = std::map<Category, std::set<const Theorem*>>;
using CategorySet = std::set<Category>;

/**
 * New theorems must call this to register themselves
 */
Theorem& addTheorem(std::string name, std::string description, const TheoremFunction& func);
/**
 * Theorems belong to categories.
 * That allows us to easily group up and report on things depending on where in the stack they belong
 * @param theorem The theorem as returned by `addTheorem`
 * @param category The category to add the theorem to. A theorem can belong to multiple categories.
 */
void categoriseTheorem(Theorem& theorem, Category category);
/**
 * Theorems can be tagged so they are easy to group up
 * @param theorem Theorem to tag.
 * @param tag Tag to apply. Tagging is idempotent.
 */
void tagTheorem(Theorem& theorem, const Tag& tag);
const TheoremMap& allTheorems();
const std::set<const Theorem*>& allTheoremsInCategory(Category type);
const CategorySet& allCategories();
std::string_view allCategoriesAsString();
}