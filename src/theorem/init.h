#pragma once
#include "theorem.h"
#include <functional>

namespace dbprove::theorem {
using TheoremMap = std::map<std::string, std::unique_ptr<Theorem>>;

/**
 * New theorem classes must add themselves here
 */
void addTheorem(Type type, std::string name, std::string description, const TheoremFunction& func);
const TheoremMap& allTheorems();
const std::set<const Theorem*>& allTheoremsInType(Type type);
}