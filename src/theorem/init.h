#pragma once
#include <dbprove/theorem/theorem.h>
#include <map>
#include <functional>
#include <optional>

namespace dbprove::theorem {
using TheoremMap = std::map<std::string, std::unique_ptr<Theorem>>;
using CategoryMap = std::map<Category, std::set<const Theorem*>>;
using CategorySet = std::set<Category>;

/**
 * New theorems must call this to register themselves
 */
Theorem& addTheorem(std::string name, std::string description, const TheoremFunction& func,
                    std::optional<sql::RowCount> expected_row_count,
                    std::optional<std::string> display_name);
void categoriseTheorem(Theorem& theorem, Category category);
/**
 * Theorems can be tagged so they are easy to group up
 * @param theorem Theorem to tag.
 * @param tag Tag to apply. Tagging is idempotent.
 */
void tagTheorem(Theorem& theorem, const Tag& tag);
void requireStorageVariant(Theorem& theorem, dbprove::StorageVariant variant);
const TheoremMap& allTheorems();
const std::set<const Theorem*>& allTheoremsInCategory(Category type);
const CategorySet& allCategories();
std::string_view allCategoriesAsString();
}
