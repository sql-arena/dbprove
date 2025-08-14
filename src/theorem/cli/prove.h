#pragma once
#include <string>
#include <vector>
#include "sql/Engine.h"
#include "sql/Credential.h"
#include "theorem/types.h"

namespace cli {
    void prove(const std::vector<std::string>&, TheoremState& credentials);
}
