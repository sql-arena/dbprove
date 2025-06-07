#pragma once
#include <string>
#include <vector>
#include "sql/Engine.h"
#include "sql/Credential.h"

namespace cli {
    void prove(const std::vector<std::string>&, sql::Engine engine, const sql::Credential& credentials);
}
