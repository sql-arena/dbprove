#pragma once

#include <cstddef>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace sql::clickhouse {
class Connection;

struct InSubqueryRelationGuess {
  std::string lhs_key;
  std::string rhs_expression;
  std::string rhs_table;
};

struct CorrelatedExistsRelationGuess {
  std::string left_table;
  std::string left_column;
  std::string right_table;
  std::string right_column;
};

std::string fetchClickHouseExplainQueryTree(Connection& connection,
                                            std::string_view statement,
                                            std::string_view artifact_name,
                                            const std::optional<std::string>& cached_query_tree);

size_t countUncorrelatedScalarSubqueriesInQueryTree(std::string_view query_tree);

std::vector<InSubqueryRelationGuess> guessInSubqueryRelationsFromQueryTree(std::string_view query_tree);
std::vector<CorrelatedExistsRelationGuess> guessCorrelatedExistsRelationsFromQueryTree(std::string_view query_tree);
} // namespace sql::clickhouse
