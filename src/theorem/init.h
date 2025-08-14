#pragma once
#include "theorem.h"
#include <functional>

namespace dbprove::theorem {
using TheoremMap = std::map<std::string, Theorem>;

/**
 * New theorem classes must add themselves here
 */
void init();
void addTheorem(Type type, std::string name, std::string description, const TheoremFunction& func);
const TheoremMap& allTheorems();
}