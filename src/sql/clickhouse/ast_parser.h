#pragma once

#include <map>
#include <optional>
#include <string>
#include <string_view>

#include "sql.h"

namespace sql::clickhouse {
class Connection;

using ScopedAstAliases = std::map<int, std::map<std::string, std::vector<std::string>>>;

std::map<std::string, std::string> guessSetsFromAst(const std::string_view ast_explain,
                                                    const EngineDialect* dialect = nullptr);
ScopedAstAliases guessScopedAliasesFromAst(const std::string_view ast_explain);

std::string fetchClickHouseExplainAst(Connection& connection,
                                      std::string_view statement,
                                      std::string_view artifact_name,
                                      const std::optional<std::string>& cached_ast);

void fetchAstAndGuessSets(Connection& connection,
                         const std::string_view statement,
                         const std::string_view artifact_name,
                         const std::optional<std::string>& cached_ast,
                         std::map<std::string, std::string>& guessed_sets,
                         ScopedAstAliases* scoped_aliases,
                         const EngineDialect* dialect,
                         bool force_fetch_ast);
} // namespace sql::clickhouse
