#include "connection.h"
#include <explain_nodes.h>
#include <nlohmann/json.hpp>
#include <array>
#include <cstdlib>
#include <functional>
#include <limits>
#include <optional>
#include <ranges>
#include <set>
#include <sstream>
#include <unordered_map>

#include <dbprove/common/string.h>
#include "dialect.h"
#include "explain/join.h"
#include "explain/materialise.h"
#include "explain/plan.h"
#include "explain/group_by.h"
#include "explain/scan.h"
#include "explain/scan_materialised.h"
#include "explain/sequence.h"
#include "explain/union.h"
#include "ast_parser.h"
#include "expression_node.h"
#include "plan_node.h"
#include "literals.h"
#include "query_tree_parser.h"
#include <plog/Log.h>

namespace sql::clickhouse {
using namespace sql::explain;
using namespace nlohmann;

std::string cleanFilter(std::string filter);
std::string stripSyntheticTableQualifiers(std::string expression);
bool expressionNodeTreeContainsExists(const ExpressionNode& root);

struct CorrelatedExistsRelation {
  std::string left_table;
  std::string left_column;
  std::string right_table;
  std::string right_column;
};

struct ExplainCtx;
std::string stripUnneededSyntheticQualifiers(std::string expression, const ExplainCtx& ctx);
std::string normalizeSetId(const std::string& set_id);
std::vector<const ExpressionNode*> snapshotExpressionChildren(const ExpressionNode& node) {
  std::vector<const ExpressionNode*> children;
  children.reserve(node.childCount());
  for (const auto* child : node.children()) {
    if (child != nullptr) {
      children.push_back(child);
    }
  }
  return children;
}

std::vector<const PlanNode*> snapshotPlanChildren(const PlanNode& node) {
  std::vector<const PlanNode*> children;
  children.reserve(node.childCount());
  for (const auto* child : node.children()) {
    if (child != nullptr) {
      children.push_back(child);
    }
  }
  return children;
}

void collectPlanNodeIds(const PlanNode& root, std::set<std::string>& out_ids) {
  std::vector<const PlanNode*> stack;
  stack.push_back(&root);
  while (!stack.empty()) {
    const auto* current = stack.back();
    stack.pop_back();
    if (current == nullptr) {
      continue;
    }
    if (!current->node_id.empty()) {
      out_ids.insert(current->node_id);
    }
    for (const auto* child : current->children()) {
      if (child != nullptr) {
        stack.push_back(child);
      }
    }
  }
}

std::string canonicalJoinKeyBase(std::string value) {
  value = cleanExpression(trim_string(std::move(value)));
  const auto dot = value.find_last_of('.');
  if (dot != std::string::npos && dot + 1 < value.size()) {
    value = value.substr(dot + 1);
  }
  const auto upper = to_upper(value);
  auto strip_suffix = [&](const std::string_view marker) {
    auto pos = upper.rfind(marker);
    if (pos == std::string::npos) {
      return false;
    }
    // Handle keys like k_right_103 / k_left_88 by trimming trailing numeric id first.
    const auto marker_with_id = std::string(marker) + "_";
    pos = upper.rfind(marker_with_id);
    if (pos == std::string::npos) {
      return false;
    }
    const auto id_start = pos + marker_with_id.size();
    if (id_start >= upper.size()) {
      return false;
    }
    const auto id = upper.substr(id_start);
    if (!std::all_of(id.begin(), id.end(), [](const char c) {
      return std::isdigit(static_cast<unsigned char>(c)) != 0;
    })) {
      return false;
    }
    value = value.substr(0, pos);
    return true;
  };
  if (!strip_suffix("_LEFT_")) {
    (void)strip_suffix("_RIGHT_");
    auto strip_plain = [&](const std::string_view marker) {
      const auto p = upper.rfind(marker);
      if (p == std::string::npos || p + marker.size() != upper.size()) {
        return false;
      }
      value = value.substr(0, p);
      return true;
    };
    if (!strip_plain("_LEFT")) {
      (void)strip_plain("_RIGHT");
    }
  }
  return value;
}

std::string unqualifyName(std::string value) {
  value = cleanExpression(trim_string(std::move(value)));
  const auto dot = value.find_last_of('.');
  if (dot != std::string::npos && dot + 1 < value.size()) {
    value = value.substr(dot + 1);
  }
  return value;
}

enum class KeyDisambiguationSide {
  NONE,
  LEFT,
  RIGHT
};

KeyDisambiguationSide keySide(const std::string& value) {
  const auto upper = to_upper(unqualifyName(value));
  if (upper.contains("_LEFT_") || upper.ends_with("_LEFT")) {
    return KeyDisambiguationSide::LEFT;
  }
  if (upper.contains("_RIGHT_") || upper.ends_with("_RIGHT")) {
    return KeyDisambiguationSide::RIGHT;
  }
  return KeyDisambiguationSide::NONE;
}

bool isSimpleSqlIdentifier(const std::string& value) {
  if (value.empty()) {
    return false;
  }
  if (!(std::isalpha(static_cast<unsigned char>(value.front())) || value.front() == '_')) {
    return false;
  }
  return std::all_of(value.begin(), value.end(), [](const char c) {
    return std::isalnum(static_cast<unsigned char>(c)) != 0 || c == '_';
  });
}

std::set<std::string> collectPlanNodeOutputNames(const PlanNode& node) {
  std::set<std::string> out;
  for (const auto& header : node.headers) {
    if (!header.name.empty()) {
      auto candidate = unqualifyName(header.name);
      if (to_upper(candidate).contains("EXISTS(") || !isSimpleSqlIdentifier(candidate)) {
        continue;
      }
      out.insert(candidate);
    }
  }
  for (const auto& expr : node.output_expressions) {
    if (!expr.output_name.empty()) {
      auto candidate = unqualifyName(expr.output_name);
      if (to_upper(candidate).contains("EXISTS(") || !isSimpleSqlIdentifier(candidate)) {
        continue;
      }
      out.insert(candidate);
    }
  }
  return out;
}

std::optional<std::string> findOutputByTokenOrBase(const std::set<std::string>& outputs, const std::string& token) {
  const auto cleaned = unqualifyName(token);
  if (cleaned.empty()) {
    return std::nullopt;
  }
  if (!isSimpleSqlIdentifier(cleaned) || to_upper(cleaned).contains("EXISTS(")) {
    return std::nullopt;
  }
  if (outputs.contains(cleaned)) {
    return cleaned;
  }
  const auto token_base = canonicalJoinKeyBase(cleaned);
  if (token_base.empty()) {
    return std::nullopt;
  }
  const auto token_side = keySide(cleaned);
  std::vector<std::string> candidates;
  for (const auto& name : outputs) {
    if (canonicalJoinKeyBase(name) == token_base) {
      candidates.push_back(name);
    }
  }
  if (candidates.empty()) {
    return std::nullopt;
  }
  if (token_side != KeyDisambiguationSide::NONE) {
    for (const auto& c : candidates) {
      if (keySide(c) == token_side) {
        return c;
      }
    }
  }
  // Default to RIGHT when both sides are present; this is the safe side for
  // correlated semi/anti rewrites where inner filters carry left/right keys.
  for (const auto& c : candidates) {
    if (keySide(c) == KeyDisambiguationSide::RIGHT) {
      return c;
    }
  }
  for (const auto& c : candidates) {
    if (keySide(c) == KeyDisambiguationSide::LEFT) {
      return c;
    }
  }
  return candidates.front();
}

std::vector<std::string> extractClauseLeafTokens(const ExpressionNode& expr) {
  if (expr.kind == ExpressionNode::Kind::ALIAS && expr.childCount() == 1 && expr.firstChild() != nullptr) {
    return extractClauseLeafTokens(*expr.firstChild());
  }
  if (expr.kind == ExpressionNode::Kind::REFERENCE) {
    if (expr.references != nullptr) {
      return extractClauseLeafTokens(*expr.references);
    }
    if (expr.linked_child_output_root != nullptr) {
      return extractClauseLeafTokens(*expr.linked_child_output_root);
    }
    if (expr.childCount() == 1 && expr.firstChild() != nullptr) {
      return extractClauseLeafTokens(*expr.firstChild());
    }
    auto token = cleanExpression(trim_string(expr.renderSql()));
    if (!token.empty()) {
      return {token};
    }
    return {};
  }
  if (expr.childCount() == 0) {
    auto token = expr.linked_child_output_name;
    if (token.empty()) {
      token = expr.output_name;
    }
    if (token.empty()) {
      token = expr.source_name;
    }
    token = cleanExpression(trim_string(std::move(token)));
    if (!token.empty()) {
      return {token};
    }
    return {};
  }
  if (expr.kind == ExpressionNode::Kind::FUNCTION) {
    std::vector<std::string> tokens;
    for (const auto* child : expr.children()) {
      if (child == nullptr) {
        continue;
      }
      auto part = extractClauseLeafTokens(*child);
      tokens.insert(tokens.end(), part.begin(), part.end());
    }
    return tokens;
  }
  if (expr.childCount() == 1 && expr.firstChild() != nullptr) {
    return extractClauseLeafTokens(*expr.firstChild());
  }
  return {};
}

std::string buildSemiAntiConditionFromChildOutputs(const PlanNode& join_plan_node, const Join& join_node) {
  if (join_plan_node.childCount() < 2 || join_node.childCount() < 2) {
    return "";
  }
  const auto* first_plan = join_plan_node.firstChild();
  const auto* last_plan = join_plan_node.lastChild();
  if (first_plan == nullptr || last_plan == nullptr) {
    return "";
  }
  const auto first_alias = join_node.firstChild()->subquerySQLAlias();
  const auto last_alias = join_node.lastChild()->subquerySQLAlias();
  if (first_alias.empty() || last_alias.empty()) {
    return "";
  }
  const auto first_outputs = collectPlanNodeOutputNames(*first_plan);
  const auto last_outputs = collectPlanNodeOutputNames(*last_plan);
  if (first_outputs.empty() || last_outputs.empty()) {
    return "";
  }

  std::vector<std::string> predicates;
  for (const auto& clause : join_plan_node.clauses) {
    if (clause.kind != ExpressionNode::Kind::FUNCTION || to_upper(clause.function_name) != "EQUALS" ||
        clause.childCount() != 2 || clause.firstChild() == nullptr || clause.lastChild() == nullptr) {
      continue;
    }
    const auto lhs_tokens = extractClauseLeafTokens(*clause.firstChild());
    const auto rhs_tokens = extractClauseLeafTokens(*clause.lastChild());
    if (lhs_tokens.empty() || rhs_tokens.empty() || lhs_tokens.size() != rhs_tokens.size()) {
      continue;
    }
    for (size_t i = 0; i < lhs_tokens.size(); ++i) {
      const auto lhs_first = findOutputByTokenOrBase(first_outputs, lhs_tokens[i]);
      const auto lhs_last = findOutputByTokenOrBase(last_outputs, lhs_tokens[i]);
      const auto rhs_first = findOutputByTokenOrBase(first_outputs, rhs_tokens[i]);
      const auto rhs_last = findOutputByTokenOrBase(last_outputs, rhs_tokens[i]);

      std::optional<std::string> first_col;
      std::optional<std::string> last_col;
      if (lhs_first.has_value() && rhs_last.has_value()) {
        first_col = lhs_first;
        last_col = rhs_last;
      } else if (rhs_first.has_value() && lhs_last.has_value()) {
        first_col = rhs_first;
        last_col = lhs_last;
      } else {
        const auto lhs_upper = to_upper(lhs_tokens[i]);
        const auto rhs_upper = to_upper(rhs_tokens[i]);
        const bool lhs_exists_wrapped = lhs_upper.contains("EXISTS(");
        const bool rhs_exists_wrapped = rhs_upper.contains("EXISTS(");
        if (lhs_exists_wrapped || rhs_exists_wrapped) {
          const auto& correlated_token = lhs_exists_wrapped ? lhs_tokens[i] : rhs_tokens[i];
          const auto common_first = findOutputByTokenOrBase(first_outputs, correlated_token);
          const auto common_last = findOutputByTokenOrBase(last_outputs, correlated_token);
          if (common_first.has_value() && common_last.has_value()) {
            first_col = common_first;
            last_col = common_last;
          }
        }
      }
      if (!first_col.has_value() || !last_col.has_value()) {
        continue;
      }
      predicates.push_back(first_alias + "." + *first_col + " = " + last_alias + "." + *last_col);
    }
  }

  if (predicates.empty()) {
    return "";
  }
  std::string out;
  for (size_t i = 0; i < predicates.size(); ++i) {
    if (i > 0) {
      out += " AND ";
    }
    out += predicates[i];
  }
  return cleanExpression(out);
}

std::string deriveSemiAntiConditionFromQueryTreeRelations(
    const PlanNode& join_plan_node,
    const Join& join_node,
    const std::vector<CorrelatedExistsRelation>& correlated_exists_relations) {
  if (!join_node.isSemiOrAnti() || join_plan_node.childCount() < 2 || join_node.childCount() < 2) {
    return "";
  }
  const auto* first_plan = join_plan_node.firstChild();
  const auto* last_plan = join_plan_node.lastChild();
  if (first_plan == nullptr || last_plan == nullptr) {
    return "";
  }

  const auto first_alias = join_node.firstChild()->subquerySQLAlias();
  const auto last_alias = join_node.lastChild()->subquerySQLAlias();
  if (first_alias.empty() || last_alias.empty()) {
    return "";
  }

  const auto first_outputs = collectPlanNodeOutputNames(*first_plan);
  const auto last_outputs = collectPlanNodeOutputNames(*last_plan);
  if (first_outputs.empty() || last_outputs.empty()) {
    return "";
  }

  std::set<std::string> clause_key_bases;
  for (const auto& clause : join_plan_node.clauses) {
    for (const auto& token : extractClauseLeafTokens(clause)) {
      const auto key_base = canonicalJoinKeyBase(token);
      if (!key_base.empty()) {
        clause_key_bases.insert(to_upper(key_base));
      }
    }
  }
  if (clause_key_bases.empty()) {
    return "";
  }

  std::vector<std::string> predicates;
  std::set<std::string> seen;
  auto maybe_add_predicate = [&](const std::string& first_token, const std::string& last_token) {
    const auto first_col = findOutputByTokenOrBase(first_outputs, first_token);
    const auto last_col = findOutputByTokenOrBase(last_outputs, last_token);
    if (!first_col.has_value() || !last_col.has_value()) {
      return;
    }
    const auto predicate = cleanExpression(first_alias + "." + *first_col + " = " + last_alias + "." + *last_col);
    if (!predicate.empty() && seen.insert(to_upper(predicate)).second) {
      predicates.push_back(predicate);
    }
  };

  for (const auto& relation : correlated_exists_relations) {
    const auto left_key = to_upper(canonicalJoinKeyBase(relation.left_column));
    const auto right_key = to_upper(canonicalJoinKeyBase(relation.right_column));
    if (clause_key_bases.contains(left_key)) {
      maybe_add_predicate(relation.left_column, relation.right_column);
      maybe_add_predicate(relation.right_column, relation.left_column);
    }
    if (clause_key_bases.contains(right_key)) {
      maybe_add_predicate(relation.left_column, relation.right_column);
      maybe_add_predicate(relation.right_column, relation.left_column);
    }
  }

  if (predicates.empty()) {
    return "";
  }
  std::string out;
  for (size_t i = 0; i < predicates.size(); ++i) {
    if (i > 0) {
      out += " AND ";
    }
    out += predicates[i];
  }
  return cleanExpression(out);
}

std::string deriveJoinConditionFromLineage(const PlanNode& join_plan_node,
                                           const Join& join_node,
                                           const std::vector<CorrelatedExistsRelation>& correlated_exists_relations) {
  if (join_plan_node.clauses.empty() || join_node.childCount() < 2) {
    return "";
  }

  const auto plan_children = snapshotPlanChildren(join_plan_node);
  if (plan_children.size() < 2) {
    return "";
  }

  std::set<std::string> left_plan_ids;
  std::set<std::string> right_plan_ids;
  collectPlanNodeIds(*plan_children[0], left_plan_ids);
  collectPlanNodeIds(*plan_children[1], right_plan_ids);

  const auto left_alias = join_node.firstChild()->subquerySQLAlias();
  const auto right_alias = join_node.lastChild()->subquerySQLAlias();
  if (left_alias.empty() || right_alias.empty()) {
    return "";
  }

  const bool is_semi_or_anti = join_node.isSemiOrAnti();
  const auto query_tree_correlation = deriveSemiAntiConditionFromQueryTreeRelations(
    join_plan_node,
    join_node,
    correlated_exists_relations);
  if (is_semi_or_anti && !query_tree_correlation.empty()) {
    return query_tree_correlation;
  }
  const auto structural = is_semi_or_anti
                            ? buildSemiAntiConditionFromChildOutputs(join_plan_node, join_node)
                            : std::string{};
  if (is_semi_or_anti && !structural.empty()) {
    return structural;
  }
  const bool has_overlapping_plan_ids = std::ranges::any_of(left_plan_ids, [&](const auto& plan_id) {
    return right_plan_ids.contains(plan_id);
  });
  const bool has_exists_wrapped_clause = is_semi_or_anti && std::ranges::any_of(
    join_plan_node.clauses,
    [](const auto& clause) {
      return to_upper(clause.renderSql()).contains("EXISTS(");
    });
  if (is_semi_or_anti && !structural.empty() && (has_overlapping_plan_ids || has_exists_wrapped_clause)) {
    return structural;
  }

  std::map<std::string, std::string> aliases_by_plan_node_id;
  for (const auto& left_plan_id : left_plan_ids) {
    aliases_by_plan_node_id[left_plan_id] = left_alias;
  }
  for (const auto& right_plan_id : right_plan_ids) {
    aliases_by_plan_node_id[right_plan_id] = right_alias;
  }

  std::string condition;
  for (size_t i = 0; i < join_plan_node.clauses.size(); ++i) {
    const auto rendered = join_plan_node.clauses[i].renderSql(aliases_by_plan_node_id);
    if (rendered.empty()) {
      continue;
    }
    if (i > 0) {
      condition += " AND ";
    }
    condition += rendered;
  }
  if (is_semi_or_anti && condition.empty() && !structural.empty()) {
    return structural;
  }
  return cleanExpression(condition);
}

struct ExplainCtx {
  ExplainCtx()
    : dialect(*this) {
  }

  std::map<std::string, std::vector<std::string>> filterSets;
  mutable std::map<std::string, std::string> filterSetById;
  mutable std::map<std::string, size_t> nextFilterSetOffsetByKey;
  std::set<const Node*> anti_join_hints;
  std::vector<CorrelatedExistsRelation> correlated_exists_relations;
  std::unique_ptr<PlanNode> resolved_json_plan_root;
  std::map<std::string, PlanNode*> resolved_json_plan_nodes_by_id;
  ClickHouseDialect dialect;

  void addFilterSet(const std::string& key, const std::string& values) {
    if (!filterSets.contains(key)) {
      filterSets[key] = {};
    }
    auto& options = filterSets[key];
    if (options.empty() || options.back() != values) {
      options.push_back(values);
    }
  }

  std::optional<std::string> resolveSetReplacementByKey(const std::string& key, const std::string& set_id_raw) const {
    const auto set_id = to_lower(set_id_raw);
    std::vector<std::string> key_candidates;
    key_candidates.push_back(key);
    key_candidates.push_back(to_upper(stripSyntheticTableQualifiers(key)));
    {
      auto tokens = tokenize(key);
      if (tokens.size() == 6 &&
          tokens[0].type == Token::Type::Function &&
          to_upper(tokens[0].value) == "LEFT" &&
          tokens[1].type == Token::Type::LeftParen &&
          tokens[2].type == Token::Type::Literal &&
          tokens[3].type == Token::Type::Comma &&
          tokens[5].type == Token::Type::RightParen) {
        key_candidates.push_back(to_upper(cleanExpression(tokens[2].value)));
      }
    }

    auto resolve_for_key = [&](const std::string& k) {
      if (!filterSetById.contains(set_id) && filterSets.contains(k) && !filterSets.at(k).empty()) {
        const auto& options = filterSets.at(k);
        const auto offset = nextFilterSetOffsetByKey[k];
        const auto use_idx = std::min(offset, options.size() - 1);
        filterSetById[set_id] = options[use_idx];
        nextFilterSetOffsetByKey[k] = offset + 1;
      }
    };

    for (const auto& candidate : key_candidates) {
      if (filterSetById.contains(set_id)) {
        break;
      }
      resolve_for_key(candidate);
    }
    if (!filterSetById.contains(set_id)) {
      return std::nullopt;
    }
    return filterSetById.at(set_id);
  }

  std::string replaceSets(std::string string_explain, const std::vector<ExpressionNode>* expression_roots = nullptr) const {
    if (expression_roots == nullptr || expression_roots->empty()) {
      return string_explain;
    }

    auto normalize_set_symbol = [](const std::string& raw) -> std::string {
      if (raw.empty()) {
        return "";
      }
      const auto upper = to_upper(raw);
      if (!upper.starts_with("__SET_")) {
        return "";
      }
      const auto suffix = raw.substr(6);
      const auto first_underscore = suffix.find('_');
      if (first_underscore == std::string::npos) {
        return raw;
      }
      const auto type_candidate = suffix.substr(0, first_underscore);
      const auto maybe_set_id = suffix.substr(first_underscore + 1);
      if (type_candidate.empty() || maybe_set_id.empty()) {
        return raw;
      }
      const auto type_is_identifier = std::ranges::all_of(type_candidate, [](const char c) {
        return std::isalnum(static_cast<unsigned char>(c)) != 0;
      });
      if (type_is_identifier && std::isalpha(static_cast<unsigned char>(type_candidate.front())) != 0) {
        return "__set_" + maybe_set_id;
      }
      return raw;
    };

    auto extract_set_symbol = [&](const auto& self, const ExpressionNode& node) -> std::string {
      if (node.kind == ExpressionNode::Kind::REFERENCE && node.childCount() == 1 && node.firstChild() != nullptr) {
        return self(self, *node.firstChild());
      }
      for (const auto& candidate : {node.result_name, node.source_name, node.output_name}) {
        const auto normalized = normalize_set_symbol(candidate);
        if (!normalized.empty()) {
          return normalized;
        }
      }
      return "";
    };

    auto render_with_set_replacement = [&](const auto& self, const ExpressionNode& node) -> std::string {
      if (node.kind == ExpressionNode::Kind::REFERENCE) {
        if (node.references != nullptr) {
          return self(self, *node.references);
        }
        if (node.linked_child_output_root != nullptr) {
          return self(self, *node.linked_child_output_root);
        }
        if (node.childCount() == 1 && node.firstChild() != nullptr) {
          return self(self, *node.firstChild());
        }
      }
      if (node.kind == ExpressionNode::Kind::ALIAS && node.childCount() == 1 && node.firstChild() != nullptr) {
        return self(self, *node.firstChild());
      }
      if (node.kind == ExpressionNode::Kind::FUNCTION) {
        const auto fn = to_upper(node.function_name);
        const bool is_in = (fn == "IN" || fn == "NOTIN" || fn == "GLOBALIN" || fn == "GLOBALNOTIN");
        const auto children = snapshotExpressionChildren(node);
        if (is_in && children.size() >= 2) {
          const auto lhs_expression = self(self, *children[0]);
          const auto set_symbol = extract_set_symbol(extract_set_symbol, *children[1]);
          if (!lhs_expression.empty() && !set_symbol.empty()) {
            const auto key = to_upper(lhs_expression);
            const auto replacement_set = resolveSetReplacementByKey(key, set_symbol);
            if (replacement_set.has_value()) {
              return lhs_expression + ((fn == "NOTIN" || fn == "GLOBALNOTIN") ? " NOT IN " : " IN ") + *replacement_set;
            }
            return "TRUE";
          }
        }

        std::vector<std::string> args;
        args.reserve(node.childCount());
        for (const auto* child : children) {
          args.push_back(self(self, *child));
        }
        if (node.function_name.empty()) {
          throw ExplainException("Cannot render FUNCTION ExpressionNode without function_name");
        }
        if ((fn == "AND" || fn == "OR") && args.size() >= 2) {
          std::string rendered = "(";
          for (size_t i = 0; i < args.size(); ++i) {
            if (i > 0) {
              rendered += " ";
              rendered += fn;
              rendered += " ";
            }
            rendered += args[i];
          }
          rendered += ")";
          return rendered;
        }
        std::string rendered = node.function_name + "(";
        for (size_t i = 0; i < args.size(); ++i) {
          if (i > 0) {
            rendered += ", ";
          }
          rendered += args[i];
        }
        rendered += ")";
        return rendered;
      }

      if (!node.source_name.empty()) {
        return node.source_name;
      }
      if (!node.result_name.empty()) {
        return node.result_name;
      }
      if (!node.output_name.empty()) {
        return node.output_name;
      }
      throw ExplainException("Cannot render ExpressionNode leaf without source or result name");
    };

    const ExpressionNode* root = nullptr;
    for (auto it = expression_roots->rbegin(); it != expression_roots->rend(); ++it) {
      if (it->kind == ExpressionNode::Kind::FUNCTION) {
        root = &*it;
        break;
      }
    }
    if (root == nullptr) {
      root = &expression_roots->back();
    }
    return render_with_set_replacement(render_with_set_replacement, *root);
  };
};

std::string normalizeSetId(const std::string& set_id) {
  return "set:" + to_lower(set_id);
}


std::string ClickHouseDialect::postRender(std::string toRender) {
  return ctx_.replaceSets(toRender);
}


std::set<std::string> collectSetIdsFromExpressionNodes(const std::vector<ExpressionNode>& expression_roots) {
  std::set<std::string> set_ids;
  auto normalize_set_symbol = [](const std::string& raw) -> std::string {
    if (raw.empty()) {
      return "";
    }
    const auto upper = to_upper(raw);
    if (!upper.starts_with("__SET_")) {
      return "";
    }
    const auto suffix = raw.substr(6);
    const auto first_underscore = suffix.find('_');
    if (first_underscore == std::string::npos) {
      return raw;
    }
    const auto type_candidate = suffix.substr(0, first_underscore);
    const auto maybe_set_id = suffix.substr(first_underscore + 1);
    if (type_candidate.empty() || maybe_set_id.empty()) {
      return raw;
    }
    const auto type_is_identifier = std::ranges::all_of(type_candidate, [](const char c) {
      return std::isalnum(static_cast<unsigned char>(c)) != 0;
    });
    if (type_is_identifier && std::isalpha(static_cast<unsigned char>(type_candidate.front())) != 0) {
      return "__set_" + maybe_set_id;
    }
    return raw;
  };
  auto extract_set_symbol = [&](const auto& self, const ExpressionNode& node) -> std::string {
    if (node.kind == ExpressionNode::Kind::REFERENCE) {
      if (node.references != nullptr) {
        return self(self, *node.references);
      }
      if (node.linked_child_output_root != nullptr) {
        return self(self, *node.linked_child_output_root);
      }
      if (node.childCount() == 1 && node.firstChild() != nullptr) {
        return self(self, *node.firstChild());
      }
    }
    if (node.kind == ExpressionNode::Kind::ALIAS && node.childCount() == 1 && node.firstChild() != nullptr) {
      return self(self, *node.firstChild());
    }
    for (const auto& candidate : {node.result_name, node.source_name, node.output_name}) {
      const auto normalized = normalize_set_symbol(candidate);
      if (!normalized.empty()) {
        return normalized;
      }
    }
    return "";
  };
  std::set<const ExpressionNode*> visited;
  auto collect = [&](const auto& self, const ExpressionNode& node) -> void {
    if (visited.contains(&node)) {
      return;
    }
    visited.insert(&node);
    if (node.kind == ExpressionNode::Kind::FUNCTION) {
      const auto fn = to_upper(node.function_name);
      if ((fn == "IN" || fn == "NOTIN" || fn == "GLOBALIN" || fn == "GLOBALNOTIN") &&
          node.childCount() >= 2 && node.children()[1] != nullptr) {
        const auto set_id = extract_set_symbol(extract_set_symbol, *node.children()[1]);
        if (!set_id.empty()) {
          set_ids.insert(normalizeSetId(set_id));
        }
      }
    }
    if (node.references != nullptr) {
      self(self, *node.references);
    }
    if (node.linked_child_output_root != nullptr) {
      self(self, *node.linked_child_output_root);
    }
    for (const auto* child : node.children()) {
      if (child != nullptr) {
        self(self, *child);
      }
    }
  };
  for (const auto& root : expression_roots) {
    collect(collect, root);
  }
  return set_ids;
}

bool isSyntheticTableQualifier(const std::string& token) {
  const auto dot_pos = token.find('.');
  if (dot_pos == std::string::npos || dot_pos == 0 || dot_pos + 1 >= token.size()) {
    return false;
  }
  const auto qualifier = token.substr(0, dot_pos);
  const auto upper = to_upper(qualifier);
  if (!upper.starts_with("__TABLE") || qualifier.size() <= 7) {
    return false;
  }
  return std::ranges::all_of(qualifier.begin() + 7, qualifier.end(), [](const char c) {
    return std::isdigit(static_cast<unsigned char>(c));
  });
}

std::string stripSyntheticTableQualifiers(std::string expression) {
  static const std::regex synthetic_qualifier(R"(\b__table[0-9]+\.)", std::regex::icase);
  expression = std::regex_replace(std::move(expression), synthetic_qualifier, "");
  return trim_string(expression);
}

std::string stripUnneededSyntheticQualifiers(std::string expression, const ExplainCtx& ctx) {
  (void)ctx;
  return stripSyntheticTableQualifiers(std::move(expression));
}

std::string cleanFilter(std::string filter) {
  filter = stripClickHouseTypedLiterals(std::move(filter));
  return cleanExpression(filter);
}

bool isSyntheticAliasName(const std::string& value) {
  const auto token = cleanExpression(value);
  if (token.empty()) {
    return false;
  }
  // ClickHouse synthetic wrappers can emit expression-like alias tokens.
  // Treat those as synthetic and keep unfolding to semantic children.
  if (token.find('(') != std::string::npos || token.find(')') != std::string::npos) {
    return true;
  }
  const auto dot_pos = token.find('.');
  const auto qualifier = dot_pos == std::string::npos ? token : token.substr(0, dot_pos);
  const auto qualifier_upper = to_upper(qualifier);
  if (!qualifier_upper.starts_with("__TABLE") || qualifier.size() <= 7) {
    return false;
  }
  return std::ranges::all_of(qualifier.begin() + 7, qualifier.end(), [](const char c) {
    return std::isdigit(static_cast<unsigned char>(c));
  });
}

std::string normalizeProjectionAliasCandidate(std::string candidate) {
  candidate = cleanExpression(trim_string(std::move(candidate)));
  if (candidate.empty() || isSyntheticAliasName(candidate)) {
    return "";
  }
  if (candidate.find('.') != std::string::npos ||
      candidate.find('(') != std::string::npos ||
      candidate.find(')') != std::string::npos ||
      candidate.find(' ') != std::string::npos ||
      candidate.find(',') != std::string::npos ||
      candidate.find('-') != std::string::npos) {
    return "";
  }
  if (!(std::isalpha(static_cast<unsigned char>(candidate.front())) ||
        candidate.front() == '_')) {
    return "";
  }
  for (const auto c : candidate) {
    if (!(std::isalnum(static_cast<unsigned char>(c)) || c == '_')) {
      return "";
    }
  }
  if (candidate.starts_with("__table") || candidate.starts_with("__TABLE")) {
    return "";
  }
  return candidate;
}

std::string selectProjectionAlias(const ExpressionNode& expression) {
  for (const auto* candidate_raw : {
         &expression.output_name,
         &expression.alias_user,
         &expression.result_name,
         &expression.source_name,
       }) {
    const auto candidate = normalizeProjectionAliasCandidate(*candidate_raw);
    if (!candidate.empty()) {
      return candidate;
    }
  }
  return "";
}

std::string renderProjectionAliasSourceSql(const ExpressionNode& expression) {
  std::set<const ExpressionNode*> visited;
  const ExpressionNode* current = &expression;
  bool traversed_reference = false;
  while (current != nullptr && !visited.contains(current)) {
    visited.insert(current);
    if (current->kind == ExpressionNode::Kind::REFERENCE) {
      traversed_reference = true;
      if (current->references != nullptr) {
        current = current->references.get();
        continue;
      }
      if (current->childCount() == 1 && current->firstChild() != nullptr) {
        current = current->firstChild();
        continue;
      }
      if (current->linked_child_output_root != nullptr &&
          current->linked_child_output_root != current) {
        current = current->linked_child_output_root;
        continue;
      }
      break;
    }
    if (current->kind == ExpressionNode::Kind::ALIAS &&
        current->childCount() == 1 &&
        current->firstChild() != nullptr) {
      current = current->firstChild();
      continue;
    }
    break;
  }

  if (traversed_reference) {
    if (current != nullptr &&
        current->kind == ExpressionNode::Kind::FUNCTION) {
      return current->renderExecutableSql();
    }
    if (current != nullptr &&
        current->leaf_binding == ExpressionNode::LeafBinding::CHILD_OUTPUT &&
        !current->linked_child_output_name.empty()) {
      return stripSyntheticTableQualifiers(current->linked_child_output_name);
    }
    if (current != nullptr && !current->source_name.empty()) {
      return stripSyntheticTableQualifiers(current->source_name);
    }
    if (current != nullptr &&
        !current->output_name.empty() &&
        !isSyntheticAliasName(current->output_name)) {
      return stripSyntheticTableQualifiers(current->output_name);
    }
  }
  return expression.renderExecutableSql();
}

std::vector<std::string> executableOutputNamesForNode(const Node& node) {
  switch (node.type) {
    case NodeType::PROJECTION: {
      const auto* projection = dynamic_cast<const Projection*>(&node);
      if (projection == nullptr) {
        return {};
      }
      std::vector<std::string> names;
      if (projection->include_input_columns && projection->childCount() > 0 && projection->firstChild() != nullptr) {
        names = executableOutputNamesForNode(*projection->firstChild());
      }
      const auto& projected = projection->synthetic_columns_projected.empty()
                                ? projection->columns_projected
                                : projection->synthetic_columns_projected;
      for (const auto& column : projected) {
        names.push_back(column.hasAlias() ? column.alias : column.name);
      }
      return names;
    }
    case NodeType::GROUP_BY: {
      const auto* group_by = dynamic_cast<const GroupBy*>(&node);
      if (group_by == nullptr) {
        return {};
      }
      std::vector<std::string> names;
      const auto& group_keys = group_by->synthetic_group_keys.empty()
                                 ? group_by->group_keys
                                 : group_by->synthetic_group_keys;
      const auto& aggregates = group_by->synthetic_aggregates.empty()
                                 ? group_by->aggregates
                                 : group_by->synthetic_aggregates;
      const auto& aliases = group_by->synthetic_aggregateAliases.empty()
                              ? group_by->aggregateAliases
                              : group_by->synthetic_aggregateAliases;
      for (const auto& key : group_keys) {
        names.push_back(key.hasAlias() ? key.alias : key.name);
      }
      for (const auto& aggregate : aggregates) {
        if (aliases.contains(aggregate)) {
          names.push_back(aliases.at(aggregate));
        } else if (aggregate.hasAlias()) {
          names.push_back(aggregate.alias);
        } else {
          names.push_back(aggregate.name);
        }
      }
      return names;
    }
    case NodeType::FILTER:
    case NodeType::SORT:
    case NodeType::LIMIT:
    case NodeType::MATERIALISE:
    case NodeType::DISTRIBUTE: {
      if (node.childCount() > 0 && node.firstChild() != nullptr) {
        return executableOutputNamesForNode(*node.firstChild());
      }
      return {};
    }
    case NodeType::JOIN: {
      std::vector<std::string> names;
      for (const auto* child : node.children()) {
        if (child == nullptr) {
          continue;
        }
        auto child_names = executableOutputNamesForNode(*child);
        names.insert(names.end(), child_names.begin(), child_names.end());
      }
      return names;
    }
    default:
      return {};
  }
}

std::vector<std::string> projectionSourceCandidatesFromLineage(const ExpressionNode& expression) {
  std::vector<std::string> candidates;
  std::set<std::string> seen_upper;
  std::set<const ExpressionNode*> visited;
  const ExpressionNode* current = &expression;
  bool traversed_reference = false;

  auto add_candidate = [&](std::string raw) {
    raw = normalizeProjectionAliasCandidate(stripSyntheticTableQualifiers(std::move(raw)));
    if (raw.empty()) {
      return;
    }
    const auto upper = to_upper(raw);
    if (!seen_upper.insert(upper).second) {
      return;
    }
    candidates.push_back(std::move(raw));
  };

  while (current != nullptr && !visited.contains(current)) {
    visited.insert(current);
    if (current->kind == ExpressionNode::Kind::REFERENCE) {
      traversed_reference = true;
    }
    if (traversed_reference) {
      add_candidate(current->alias_user);
      add_candidate(current->linked_child_output_name);
      add_candidate(current->output_name);
      add_candidate(current->result_name);
      add_candidate(current->source_name);
    }
    if (current->kind == ExpressionNode::Kind::REFERENCE) {
      if (current->references != nullptr) {
        current = current->references.get();
        continue;
      }
      if (current->childCount() == 1 && current->firstChild() != nullptr) {
        current = current->firstChild();
        continue;
      }
      if (current->linked_child_output_root != nullptr &&
          current->linked_child_output_root != current) {
        current = current->linked_child_output_root;
        continue;
      }
      break;
    }
    if (current->kind == ExpressionNode::Kind::ALIAS &&
        current->childCount() == 1 &&
        current->firstChild() != nullptr) {
      current = current->firstChild();
      continue;
    }
    break;
  }
  return candidates;
}

void retargetProjectionColumnsToChildLineage(Projection& projection,
                                             const PlanNode& plan_node) {
  if (projection.childCount() == 0 || projection.firstChild() == nullptr) {
    return;
  }
  const auto child_output_names = executableOutputNamesForNode(*projection.firstChild());
  if (child_output_names.empty()) {
    return;
  }
  std::set<std::string> child_output_names_upper;
  for (const auto& name : child_output_names) {
    child_output_names_upper.insert(to_upper(name));
  }

  std::vector<Column> rewritten;
  rewritten.reserve(projection.columns_projected.size());
  size_t projected_index = 0;
  for (const auto& expr : plan_node.output_expressions) {
    if (expressionNodeTreeContainsExists(expr)) {
      continue;
    }
    if (projected_index >= projection.columns_projected.size()) {
      break;
    }
    const auto alias = selectProjectionAlias(expr);
    std::optional<Column> replacement;
    for (const auto& candidate : projectionSourceCandidatesFromLineage(expr)) {
      if (!child_output_names_upper.contains(to_upper(candidate))) {
        continue;
      }
      if (!alias.empty() && to_upper(candidate) != to_upper(alias)) {
        replacement.emplace(candidate, alias);
      } else {
        replacement.emplace(candidate);
      }
      break;
    }
    if (replacement.has_value()) {
      rewritten.push_back(std::move(replacement.value()));
    } else {
      rewritten.push_back(projection.columns_projected[projected_index]);
    }
    ++projected_index;
  }

  if (rewritten.size() == projection.columns_projected.size()) {
    projection.columns_projected = std::move(rewritten);
  }
}

bool expressionNodeTreeContainsExists(const ExpressionNode& root) {
  std::set<const ExpressionNode*> visited;
  std::vector<const ExpressionNode*> stack;
  stack.push_back(&root);
  while (!stack.empty()) {
    const auto* current = stack.back();
    stack.pop_back();
    if (current == nullptr || visited.contains(current)) {
      continue;
    }
    visited.insert(current);
    if (current->isExists()) {
      return true;
    }
    if (current->references != nullptr) {
      stack.push_back(current->references.get());
    }
    for (const auto* child : current->children()) {
      if (child != nullptr) {
        stack.push_back(child);
      }
    }
  }
  return false;
}

bool isExplicitNonSyntheticAliasProjection(const ExpressionNode& expression) {
  if (expression.kind != ExpressionNode::Kind::ALIAS) {
    return false;
  }
  if (expressionNodeTreeContainsExists(expression)) {
    return false;
  }
  for (const auto* candidate_raw : {&expression.output_name, &expression.result_name, &expression.source_name}) {
    const auto candidate = cleanExpression(*candidate_raw);
    if (candidate.empty()) {
      continue;
    }
    if (!isSyntheticAliasName(candidate)) {
      return true;
    }
  }
  return false;
}

std::string effectiveScanAliasForNode(const std::string& node_id,
                                      const std::string& table_name,
                                      const int json_subquery_depth,
                                      ExplainCtx& ctx) {
  (void)node_id;
  (void)table_name;
  (void)json_subquery_depth;
  (void)ctx;
  return "";
}

std::optional<std::string> firstScanTableNameInJsonSubtree(const json& node_json) {
  if (!node_json.is_object()) {
    return std::nullopt;
  }
  const auto node_type = node_json.value("Node Type", "");
  if ((node_type == "ReadFromMergeTree" || node_type == "ReadFromStorage") &&
      node_json.contains("Description") && node_json["Description"].is_string()) {
    return cleanExpression(node_json["Description"].get<std::string>());
  }
  if (!node_json.contains("Plans") || !node_json["Plans"].is_array()) {
    return std::nullopt;
  }
  for (const auto& child : node_json["Plans"]) {
    const auto candidate = firstScanTableNameInJsonSubtree(child);
    if (candidate.has_value() && !candidate->empty()) {
      return candidate;
    }
  }
  return std::nullopt;
}

bool isLikelyDegenerateExistsJoinCondition(const std::string& condition) {
  auto cleaned = cleanExpression(condition);
  if (cleaned.empty() || to_upper(cleaned) == "TRUE" || cleaned == "1=1") {
    return true;
  }
  auto tokens = tokenize(cleaned);
  if (tokens.empty()) {
    return true;
  }
  int depth = 0;
  std::optional<size_t> eq_idx;
  for (size_t i = 0; i < tokens.size(); ++i) {
    if (tokens[i].type == Token::Type::LeftParen) {
      ++depth;
      continue;
    }
    if (tokens[i].type == Token::Type::RightParen) {
      --depth;
      continue;
    }
    if (depth == 0 && tokens[i].type == Token::Type::Operator && tokens[i].value == "=") {
      eq_idx = i;
      break;
    }
  }
  if (!eq_idx.has_value() || *eq_idx == 0 || *eq_idx + 1 >= tokens.size()) {
    return false;
  }
  std::vector<Token> lhs_tokens(tokens.begin(), tokens.begin() + static_cast<ssize_t>(*eq_idx));
  std::vector<Token> rhs_tokens(tokens.begin() + static_cast<ssize_t>(*eq_idx + 1), tokens.end());
  auto lhs = cleanExpression(render(lhs_tokens));
  auto rhs = cleanExpression(render(rhs_tokens));
  if (lhs.empty() || rhs.empty()) {
    return false;
  }
  if (lhs == rhs) {
    return true;
  }
  auto suffix_after_dot = [](const std::string& expr) {
    const auto pos = expr.find_last_of('.');
    if (pos == std::string::npos || pos + 1 >= expr.size()) {
      return std::string{};
    }
    return expr.substr(pos + 1);
  };
  const auto lhs_suffix = suffix_after_dot(lhs);
  const auto rhs_suffix = suffix_after_dot(rhs);
  return !lhs_suffix.empty() && lhs_suffix == rhs_suffix;
}

std::optional<std::string> deriveCorrelatedExistsJoinConditionFromQueryTree(const json& node_json, const ExplainCtx& ctx) {
  if (!node_json.contains("Plans") || !node_json["Plans"].is_array() || node_json["Plans"].size() < 2) {
    return std::nullopt;
  }
  const auto left_table = firstScanTableNameInJsonSubtree(node_json["Plans"][0]);
  const auto right_table = firstScanTableNameInJsonSubtree(node_json["Plans"][1]);
  if (!left_table.has_value() || !right_table.has_value()) {
    return std::nullopt;
  }
  const auto left_table_norm = to_upper(left_table.value());
  const auto right_table_norm = to_upper(right_table.value());

  for (const auto& relation : ctx.correlated_exists_relations) {
    const auto rel_left = to_upper(relation.left_table);
    const auto rel_right = to_upper(relation.right_table);
    if (rel_left == left_table_norm && rel_right == right_table_norm) {
      return cleanExpression(relation.left_column + " = " + relation.right_column);
    }
    if (rel_left == right_table_norm && rel_right == left_table_norm) {
      return cleanExpression(relation.right_column + " = " + relation.left_column);
    }
  }
  return std::nullopt;
}

std::string normalizeJoinClauseFromJson(const std::string& clause, const ExplainCtx& ctx) {
  auto cleaned = trim_string(clause);
  if (!cleaned.empty() && cleaned.front() == '[' && cleaned.back() == ']') {
    cleaned = cleaned.substr(1, cleaned.size() - 2);
  }
  cleaned = cleanExpression(cleaned);
  cleaned = stripUnneededSyntheticQualifiers(cleanFilter(cleaned), ctx);
  return cleanExpression(cleaned);
}

const ExpressionNode* findActionByResultName(const PlanNode& plan_node, const std::string& result_name) {
  if (result_name.empty()) {
    return nullptr;
  }
  const auto target = to_upper(cleanExpression(result_name));
  for (auto it = plan_node.actions.rbegin(); it != plan_node.actions.rend(); ++it) {
    if (to_upper(cleanExpression(it->result_name)) == target ||
        to_upper(cleanExpression(it->output_name)) == target) {
      return &*it;
    }
  }
  return nullptr;
}

std::string renderSingleActionExpression(const PlanNode& plan_node,
                                         const std::optional<std::string>& preferred_result_name,
                                         const std::string_view purpose) {
  if (preferred_result_name.has_value()) {
    if (const auto* preferred = findActionByResultName(plan_node, *preferred_result_name); preferred != nullptr) {
      return preferred->renderUser();
    }
  }
  if (!plan_node.actions.empty()) {
    return plan_node.actions.back().renderUser();
  }
  throw ExplainException("Missing actions for node '" + plan_node.node_id + "' (" + std::string(purpose) + ")");
}

std::string renderAndJoinExpressions(const PlanNode& plan_node,
                                     const std::vector<ExpressionNode>& expressions,
                                     const std::string_view purpose) {
  if (expressions.empty()) {
    throw ExplainException("Missing expressions for node '" + plan_node.node_id + "' (" + std::string(purpose) + ")");
  }
  std::string rendered;
  for (size_t i = 0; i < expressions.size(); ++i) {
    const auto part = expressions[i].renderUser();
    if (part.empty()) {
      throw ExplainException("Failed to render expression for node '" + plan_node.node_id + "' (" + std::string(purpose) + ")");
    }
    if (i > 0) {
      rendered += " AND ";
    }
    rendered += part;
  }
  return rendered;
}

std::unique_ptr<Node> createExplainNodeFromResolvedPlanNode(
    const PlanNode& plan_node,
    ExplainCtx& ctx,
    const int json_subquery_depth) {
  const auto& node_json = plan_node.raw_json;
  const auto& node_type = plan_node.node_type;
  std::unique_ptr<Node> node = nullptr;
  auto ch_node_id = plan_node.node_id;
  if (ch_node_id.empty()) {
    ch_node_id = node_type + ":" + node_json.value("Description", "");
  }
  try {
  if (node_type == "Expression") {
    std::vector<Column> columns_projected;
    columns_projected.reserve(plan_node.output_expressions.size());
    for (const auto& projection_expression : plan_node.output_expressions) {
      if (expressionNodeTreeContainsExists(projection_expression)) {
        continue;
      }
      const auto rendered_alias = selectProjectionAlias(projection_expression);
      auto rendered_expression = projection_expression.renderExecutableSql();
      if (!rendered_alias.empty()) {
        rendered_expression = renderProjectionAliasSourceSql(projection_expression);
      }
      if (rendered_expression.empty()) {
        throw ExplainException("Failed to render projection expression for node '" + ch_node_id + "'");
      }

      try {
        if (!rendered_alias.empty() &&
            to_upper(cleanExpression(rendered_expression)) != to_upper(rendered_alias)) {
          columns_projected.emplace_back(rendered_expression, rendered_alias);
        } else {
          columns_projected.emplace_back(rendered_expression);
        }
      } catch (const std::exception& e) {
        throw ExplainException(
          "Failed to parse projection expression for node '" + ch_node_id + "': '" + rendered_expression + "': " + e.what());
      }
    }
    if (columns_projected.empty()) {
      return nullptr;
    }
    node = std::make_unique<Projection>(columns_projected);
  } else if (node_type == "ReadFromMergeTree" || node_type == "ReadFromStorage") {
    auto table_name = node_json.value("Description", "");
    const auto scan_alias = effectiveScanAliasForNode(ch_node_id, table_name, json_subquery_depth, ctx);
    auto scan = std::make_unique<Scan>(table_name, Scan::Strategy::SCAN, scan_alias);
    const auto set_ids = collectSetIdsFromExpressionNodes(plan_node.prewhere_filter_expressions);
    const bool has_set_ids = !set_ids.empty();
    if (!plan_node.prewhere_filter_expressions.empty()) {
      auto rendered_prewhere = plan_node.renderPrewhere();
      if (has_set_ids) {
        rendered_prewhere = ctx.replaceSets(rendered_prewhere, &plan_node.prewhere_filter_expressions);
      }
      rendered_prewhere = stripUnneededSyntheticQualifiers(rendered_prewhere, ctx);
      if (!rendered_prewhere.empty()) {
        scan->setFilter(rendered_prewhere);
      }
    }
    node = std::move(scan);
  } else if (node_type == "Sorting") {
    std::vector<Column> columns_sorted;
    columns_sorted.reserve(plan_node.sort_columns.size());
    for (size_t i = 0; i < plan_node.sort_columns.size(); ++i) {
      const auto rendered = plan_node.sort_columns[i].renderUser();
      if (rendered.empty()) {
        throw ExplainException("Failed to render sort column for node '" + ch_node_id + "'");
      }
      const auto ascending = i < plan_node.unresolved_sort_ascending.size()
                               ? plan_node.unresolved_sort_ascending[i]
                               : true;
      columns_sorted.emplace_back(rendered, ascending ? Column::Sorting::ASC : Column::Sorting::DESC);
    }
    node = std::make_unique<Sort>(columns_sorted);
  } else if (node_type == "Aggregating") {
    std::vector<Column> columns_aggregated;
    std::vector<Column> columns_grouped;
    columns_grouped.reserve(plan_node.keys.size());
    for (const auto& key_expression : plan_node.keys) {
      const auto rendered = key_expression.renderSql();
      if (rendered.empty()) {
        throw ExplainException("Failed to render grouping key for node '" + ch_node_id + "'");
      }
      columns_grouped.emplace_back(rendered, &ctx.dialect);
    }
    columns_aggregated.reserve(plan_node.aggregates.size());
    for (size_t i = 0; i < plan_node.aggregates.size(); ++i) {
      const auto& aggregate_expression = plan_node.aggregates[i];
      const auto rendered = aggregate_expression.renderSql();
      if (rendered.empty()) {
        throw ExplainException("Failed to render aggregate for node '" + ch_node_id + "'");
      }
      std::string rendered_alias;
      if (!aggregate_expression.alias_user.empty()) {
        rendered_alias = cleanExpression(aggregate_expression.alias_user);
      } else if (!aggregate_expression.output_name.empty()) {
        rendered_alias = cleanExpression(aggregate_expression.output_name);
      } else if (!aggregate_expression.result_name.empty()) {
        rendered_alias = cleanExpression(aggregate_expression.result_name);
      }

      if (!rendered_alias.empty() &&
          !isSyntheticAliasName(rendered_alias) &&
          to_upper(cleanExpression(rendered)) != to_upper(rendered_alias)) {
        columns_aggregated.emplace_back(Column(rendered, rendered_alias, &ctx.dialect));
      } else {
        columns_aggregated.emplace_back(Column(rendered, &ctx.dialect));
      }
    }
    node = std::make_unique<GroupBy>(GroupBy::Strategy::HASH, columns_grouped, columns_aggregated);
  } else if (node_type == "Limit") {
    auto limit_value = node_json["Limit"].get<int64_t>();
    node = std::make_unique<Limit>(limit_value);
  } else if (node_type == "Filter") {
    if (plan_node.filter_columns.empty()) {
      throw ExplainException("Missing resolved filter columns for node '" + ch_node_id + "'");
    }
    const auto set_ids_in_filter_columns = collectSetIdsFromExpressionNodes(plan_node.filter_columns);
    const auto set_ids_in_actions = collectSetIdsFromExpressionNodes(plan_node.actions);
    std::set<std::string> set_ids = set_ids_in_filter_columns;
    set_ids.insert(set_ids_in_actions.begin(), set_ids_in_actions.end());
    const auto* set_roots = !set_ids_in_filter_columns.empty()
                              ? &plan_node.filter_columns
                              : (!set_ids_in_actions.empty() ? &plan_node.actions : nullptr);
    std::string filter_condition;
    for (size_t i = 0; i < plan_node.filter_columns.size(); ++i) {
      auto rendered_filter = plan_node.filter_columns[i].renderUser();
      if (set_roots != nullptr) {
        rendered_filter = ctx.replaceSets(rendered_filter, set_roots);
      }
      rendered_filter = stripUnneededSyntheticQualifiers(rendered_filter, ctx);
      if (rendered_filter.empty()) {
        throw ExplainException("Failed to render filter column for node '" + ch_node_id + "'");
      }
      if (i > 0) {
        filter_condition += " AND ";
      }
      filter_condition += rendered_filter;
    }
    node = std::make_unique<Selection>(filter_condition);
  } else if (node_type == "Join") {
    const auto join_strategy = node_json["Algorithm"].get<std::string>().contains("Hash")
                                 ? Join::Strategy::HASH
                                 : Join::Strategy::LOOP;
    const auto strictness = to_upper(node_json.value("Strictness", ""));
    std::string join_condition;
    for (size_t i = 0; i < plan_node.clauses.size(); ++i) {
      const auto part = plan_node.clauses[i].renderSql();
      if (part.empty()) {
        throw ExplainException("Failed to render expression for node '" + plan_node.node_id + "' (join clauses)");
      }
      if (i > 0) {
        join_condition += " AND ";
      }
      join_condition += part;
    }
    const auto has_exists_wrapper = to_upper(join_condition).contains("EXISTS(");
    if ((strictness == "ANTI" || strictness == "SEMI") &&
        (has_exists_wrapper || isLikelyDegenerateExistsJoinCondition(join_condition))) {
      const auto correlated_condition = deriveCorrelatedExistsJoinConditionFromQueryTree(node_json, ctx);
      if (correlated_condition.has_value()) {
        join_condition = correlated_condition.value();
      }
    }
    if (join_condition.empty()) {
      join_condition = "TRUE";
    }
    const auto join_type = Join::typeFromString(node_json["Type"].get<std::string>());
    node = std::make_unique<Join>(join_type, join_strategy, join_condition);
    if (strictness == "ANTI") {
      ctx.anti_join_hints.insert(node.get());
    }
  } else if (node_type == "Distinct") {
    // Represent DISTINCT as a key-only aggregate to preserve de-dup semantics.
    std::vector<Column> distinct_keys;
    for (const auto& expression : plan_node.output_expressions) {
      if (expressionNodeTreeContainsExists(expression)) {
        continue;
      }
      if (expression.output_name.empty()) {
        continue;
      }
      const auto rendered = expression.renderUser();
      if (rendered.empty()) {
        continue;
      }
      distinct_keys.emplace_back(rendered);
    }
    node = std::make_unique<GroupBy>(GroupBy::Strategy::HASH, distinct_keys, std::vector<Column>{});
  } else if (node_type == "Union") {
    node = std::make_unique<Union>(Union::Type::ALL);
  } else if (node_type == "SaveSubqueryResultToBuffer") {
    node = std::make_unique<Materialise>(node_type, -1, ch_node_id);
  } else if (node_type == "ReadFromCommonBuffer") {
    std::string producer_node_name;
    if (plan_node.common_buffer_producer != nullptr) {
      producer_node_name = plan_node.common_buffer_producer->node_id;
    }
    std::string projected_columns;
    size_t projected_count = 0;
    for (const auto& output_expression : plan_node.output_expressions) {
      if (expressionNodeTreeContainsExists(output_expression)) {
        continue;
      }
      const auto column_name = output_expression.output_name;
      if (column_name.empty()) {
        continue;
      }
      if (projected_count++ > 0) {
        projected_columns += ", ";
      }
      // We only need shape/column names so subtree COUNT(*) SQL can bind predicates.
      projected_columns += "NULL AS " + column_name;
    }
    node = std::make_unique<ScanMaterialised>(-1, projected_columns, producer_node_name);
  } else if (node_type == "CreatingSets" || node_type == "CreatingSet") {
    // These are prep nodes that simply construct layouts
    return nullptr;
  }
  if (!node) {
    throw ExplainException("Could not parse ClickHouse node of type: " + node_type);
  }

  return node;
  } catch (const std::exception& e) {
    throw ExplainException("Failed parsing ClickHouse node '" + ch_node_id + "' of type '" + node_type + "': " + e.what());
  }
}

std::unique_ptr<Node> buildExplainNode(PlanNode& plan_node, ExplainCtx& ctx,
                                       std::vector<std::unique_ptr<Node>>* sequence_nodes = nullptr,
                                       const int json_subquery_depth = 0) {
  auto snapshot_children = [](PlanNode& node) {
    std::vector<PlanNode*> snapshot;
    snapshot.reserve(node.childCount());
    for (auto* child : node.children()) {
      if (child != nullptr) {
        snapshot.push_back(child);
      }
    }
    return snapshot;
  };

  const auto& node_type = plan_node.node_type;
  if (node_type == "CreatingSets" && sequence_nodes != nullptr) {
    for (auto* child_plan : snapshot_children(plan_node)) {
      auto child_node = buildExplainNode(*child_plan, ctx, sequence_nodes, json_subquery_depth);
      if (child_node) {
        sequence_nodes->push_back(std::move(child_node));
      }
    }
    return nullptr;
  }

  auto* active_plan = &plan_node;
  auto node = createExplainNodeFromResolvedPlanNode(*active_plan, ctx, json_subquery_depth);
  while (node == nullptr) {
    // A CreatingSet may unwrap into CreatingSets after node_json reassignment below.
    // Handle it here too so nested set-prep trees can be hoisted instead of throwing on Plans > 1.
    if (active_plan->node_type == "CreatingSets" && sequence_nodes != nullptr) {
      for (auto* child_plan : snapshot_children(*active_plan)) {
        auto child_node = buildExplainNode(*child_plan, ctx, sequence_nodes, json_subquery_depth);
        if (child_node) {
          sequence_nodes->push_back(std::move(child_node));
        }
      }
      return nullptr;
    }
    if (active_plan->childCount() == 0) {
      throw std::runtime_error(
          "EXPLAIN tries to skip past a node that has no children. You must handle all leaf nodes");
    }
    if (active_plan->childCount() > 1) {
      throw std::runtime_error(
          "EXPLAIN parsing Tried to skip past a node with >1 children. You have to deal with this case");
    }
    active_plan = active_plan->firstChild();
    node = createExplainNodeFromResolvedPlanNode(*active_plan, ctx, json_subquery_depth);
  }

  if (active_plan->childCount() > 0) {
    const auto description_upper = to_upper(active_plan->description);
    const bool enters_subquery_scope = description_upper.contains("SUBQUERY");
    const auto child_subquery_depth = enters_subquery_scope ? json_subquery_depth + 1 : json_subquery_depth;
    for (auto* child_plan : snapshot_children(*active_plan)) {
      auto child_node = buildExplainNode(*child_plan, ctx, sequence_nodes, child_subquery_depth);
      if (child_node) {
        node->addChild(std::move(child_node));
      }
    }
  }

  if (node->type == NodeType::PROJECTION) {
    if (auto* projection = dynamic_cast<Projection*>(node.get()); projection != nullptr) {
      retargetProjectionColumnsToChildLineage(*projection, *active_plan);
    }
  }

  if (node->type == NodeType::JOIN) {
    auto* join_node = dynamic_cast<Join*>(node.get());
    if (join_node != nullptr) {
      const auto lineage_condition = deriveJoinConditionFromLineage(
        *active_plan,
        *join_node,
        ctx.correlated_exists_relations);
      if (!lineage_condition.empty()) {
        join_node->setSyntheticCondition(lineage_condition);
      }
    }
  }

  if (sequence_nodes != nullptr && node->type == NodeType::MATERIALISE) {
    sequence_nodes->push_back(std::move(node));
    return nullptr;
  }

  return node;
}

void linkScanMaterialisedToSourceMaterialise(Node& root) {
  std::map<std::string, const Materialise*> materialise_by_name;
  std::map<std::string, const Materialise*> materialise_by_alias;
  for (auto& n : root.depth_first()) {
    if (n.type != NodeType::MATERIALISE) {
      continue;
    }
    const auto* materialise = dynamic_cast<const Materialise*>(&n);
    if (materialise == nullptr) {
      continue;
    }
    if (!materialise->materialised_node_name.empty()) {
      materialise_by_name[materialise->materialised_node_name] = materialise;
    }
    materialise_by_alias[materialise->materialisedAlias()] = materialise;
  }
  for (auto& n : root.depth_first()) {
    if (n.type != NodeType::SCAN_MATERIALISED) {
      continue;
    }
    auto* scan_materialised = dynamic_cast<ScanMaterialised*>(&n);
    if (scan_materialised == nullptr || scan_materialised->sourceMaterialise() != nullptr) {
      continue;
    }
    const Materialise* source = nullptr;
    if (!scan_materialised->primary_node_name.empty() &&
        materialise_by_name.contains(scan_materialised->primary_node_name)) {
      source = materialise_by_name.at(scan_materialised->primary_node_name);
    } else if (materialise_by_alias.contains(scan_materialised->materialisedAlias())) {
      source = materialise_by_alias.at(scan_materialised->materialisedAlias());
    }
    if (source != nullptr) {
      scan_materialised->setSourceMaterialise(source);
    }
  }
}


std::unique_ptr<Plan> buildExplainPlan(json& json, ExplainCtx& ctx) {
  if (!json.is_array()) {
    throw ExplainException("ClickHouse plans are supposed to be inside an array");
  }
  const auto top_plan = json[0];
  if (!top_plan.contains("Plan")) {
    throw ExplainException("Invalid EXPLAIN format. Expected to find a Plans node");
  }

  std::vector<std::unique_ptr<Node>> sequence_nodes;

  /* Construct the tree*/
  if (!ctx.resolved_json_plan_root) {
    throw ExplainException("Resolved JSON plan tree must be built before explain plan construction");
  }
  auto root_node = buildExplainNode(*ctx.resolved_json_plan_root, ctx, &sequence_nodes);

  if (!sequence_nodes.empty()) {
    auto sequence = std::make_unique<Sequence>();
    for (auto& child : sequence_nodes) {
      sequence->addChild(std::move(child));
    }
    if (root_node) {
      sequence->addChild(std::move(root_node));
    }
    root_node = std::move(sequence);
    root_node->setParentToSelf();
  }

  if (!root_node) {
    throw std::runtime_error("Invalid EXPLAIN plan output, could not construct a plan");
  }
  linkScanMaterialisedToSourceMaterialise(*root_node);

  auto plan = std::make_unique<Plan>(std::move(root_node));
  plan->planning_time = 0;
  plan->execution_time = 0;
  return plan;
}

std::string fetchClickHouseExplainJson(Connection& connection,
                                       const std::string_view statement,
                                       const std::string_view artifact_name,
                                       const std::optional<std::string>& cached_json) {
  if (cached_json.has_value()) {
    PLOGI << "Using cached execution plan artifact for: " << artifact_name;
    return *cached_json;
  }

  const std::string explain_stmt = "EXPLAIN PLAN json = 1, actions = 1, header = 1, description = 1\n"
                                   + std::string(statement) + "\nFORMAT TSVRaw";
  auto json_explain = connection.fetchScalar(explain_stmt).asString();
  connection.storeArtefact(artifact_name, "json", json_explain);
  return json_explain;
}

bool pruneBroadcastPlanNodes(PlanNode& root) {
  std::map<std::string, std::vector<PlanNode*>> nodes_by_id;
  for (auto& node : root.depth_first()) {
    if (node.node_id.empty()) {
      continue;
    }
    nodes_by_id[node.node_id].push_back(&node);
  }

  size_t removed = 0;
  for (auto& [node_id, nodes] : nodes_by_id) {
    (void)node_id;
    if (nodes.size() <= 1) {
      continue;
    }
    std::ranges::sort(nodes, [](const PlanNode* lhs, const PlanNode* rhs) {
      return lhs->depth() < rhs->depth();
    });
    for (size_t i = 1; i < nodes.size(); ++i) {
      if (nodes[i] == nullptr || nodes[i]->isRoot()) {
        continue;
      }
      nodes[i]->remove();
      ++removed;
    }
  }
  return removed > 0;
}

bool applyStrictnessJoinTypePlanNodes(PlanNode& root) {
  auto replacementTypeFor = [](const std::string& raw_type, const std::string& raw_strictness) -> std::string {
    const auto strictness = to_upper(trim_string(raw_strictness));
    if (strictness != "SEMI" && strictness != "ANTI") {
      return "";
    }

    const auto type = to_upper(trim_string(raw_type));
    const bool is_right = type.contains("RIGHT");
    const bool is_left = type.contains("LEFT");

    if (strictness == "SEMI") {
      if (is_right) {
        return "right_semi";
      }
      if (is_left) {
        return "left_semi";
      }
      return "left_semi";
    }

    if (is_right) {
      return "right_anti";
    }
    if (is_left) {
      return "left_anti";
    }
    return "left_anti";
  };

  size_t rewritten = 0;
  for (auto& node : root.depth_first()) {
    if (to_upper(node.node_type) != "JOIN") {
      continue;
    }
    if (!node.raw_json.is_object()) {
      continue;
    }
    const auto strictness = node.raw_json.value("Strictness", "");
    const auto type = node.raw_json.value("Type", "");
    if (strictness.empty() || type.empty()) {
      continue;
    }
    const auto replacement = replacementTypeFor(type, strictness);
    if (replacement.empty() || to_lower(type) == replacement) {
      continue;
    }
    PLOGD << "Strictness join type rewrite node_id=" << node.node_id
          << " strictness=" << strictness
          << " type=" << type
          << " -> " << replacement;
    node.raw_json["Type"] = replacement;
    ++rewritten;
  }
  return rewritten > 0;
}



bool flipJoinChildrenPlanNodes(PlanNode& root) {
  size_t flipped = 0;
  for (auto& node : root.depth_first()) {
    if (to_upper(node.node_type) != "JOIN") {
      continue;
    }
    if (node.childCount() < 2) {
      continue;
    }
    node.reverseChildren();
    ++flipped;
  }
  return flipped > 0;
}

bool flattenCreatingSetWrappersPlanNodes(PlanNode& root) {
  std::vector<PlanNode*> wrappers;
  for (auto& node : root.bottom_up()) {
    if (to_upper(node.node_type) != "CREATINGSET" || node.isRoot()) {
      continue;
    }
    wrappers.push_back(&node);
  }

  size_t flattened = 0;
  for (auto* wrapper : wrappers) {
    if (wrapper == nullptr || wrapper->isRoot()) {
      continue;
    }
    std::string produced_set_id;
    if (wrapper->raw_json.contains("Set") && wrapper->raw_json["Set"].is_string()) {
      produced_set_id = normalizeSetId(wrapper->raw_json["Set"].get<std::string>());
    }

    std::vector<PlanNode*> children;
    for (auto* child : wrapper->children()) {
      if (child != nullptr) {
        children.push_back(child);
      }
    }
    if (!produced_set_id.empty()) {
      for (auto* child : children) {
        if (!child->raw_json.contains("__produced_set_ids") || !child->raw_json["__produced_set_ids"].is_array()) {
          child->raw_json["__produced_set_ids"] = json::array();
        }
        bool exists = false;
        for (const auto& id : child->raw_json["__produced_set_ids"]) {
          if (id.is_string() && id.get<std::string>() == produced_set_id) {
            exists = true;
            break;
          }
        }
        if (!exists) {
          child->raw_json["__produced_set_ids"].push_back(produced_set_id);
        }
      }
    }

    wrapper->remove();
    ++flattened;
  }
  return flattened > 0;
}

std::unique_ptr<PlanNode> makeInlineReadFromCommonBufferWrapper(const PlanNode& producer, const size_t ordinal) {
  auto wrapper = std::make_unique<PlanNode>();
  wrapper->node_type = "ReadFromCommonBuffer";
  wrapper->node_id = producer.node_id + "_inline_read_" + std::to_string(ordinal);
  wrapper->description = "Synthetic inline read of materialised buffer";
  wrapper->headers.clear();
  wrapper->output_expressions.clear();
  wrapper->headers.reserve(producer.headers.size());
  wrapper->output_expressions.reserve(producer.headers.size());

  for (const auto& producer_header : producer.headers) {
    if (producer_header.name.empty()) {
      continue;
    }
    wrapper->headers.emplace_back();
    auto& header = wrapper->headers.back();
    header.name = producer_header.name;
    header.type = producer_header.type;

    auto& output = wrapper->output_expressions.emplace_back();
    output.kind = ExpressionNode::Kind::COLUMN;
    output.leaf_binding = ExpressionNode::LeafBinding::NONE;
    output.plan_node_id = wrapper->node_id;
    output.output_name = header.name;
    output.output_type = header.type;
    output.result_name = header.name;
    output.source_name = header.name;
    output.setExpression(header.name);
  }

  return wrapper;
}

bool insertInlineMaterialisedReadPlanNodes(PlanNode& root) {
  std::map<const PlanNode*, size_t> consumer_count_by_producer;
  for (auto& node : root.depth_first()) {
    if (to_upper(node.node_type) != "READFROMCOMMONBUFFER") {
      continue;
    }
    if (node.common_buffer_producer == nullptr) {
      continue;
    }
    ++consumer_count_by_producer[node.common_buffer_producer];
  }

  std::vector<PlanNode*> producer_candidates;
  producer_candidates.reserve(consumer_count_by_producer.size());
  for (auto& node : root.depth_first()) {
    if (to_upper(node.node_type) != "SAVESUBQUERYRESULTTOBUFFER") {
      continue;
    }
    if (node.isRoot()) {
      continue;
    }
    if (!consumer_count_by_producer.contains(&node)) {
      continue;
    }
    const auto parent_type = to_upper(node.parent().node_type);
    if (parent_type == "CREATINGSET" || parent_type == "CREATINGSETS" || parent_type == "READFROMCOMMONBUFFER") {
      continue;
    }
    producer_candidates.push_back(&node);
  }

  size_t wrapped = 0;
  for (auto* producer : producer_candidates) {
    if (producer == nullptr || producer->isRoot()) {
      continue;
    }
    auto& parent = producer->parent();
    std::vector<std::shared_ptr<PlanNode>> original_children;
    original_children.reserve(parent.childCount());
    while (parent.childCount() > 0) {
      original_children.push_back(parent.takeChild(0));
    }

    bool replaced = false;
    for (auto& child : original_children) {
      if (child.get() != producer) {
        parent.addSharedChild(std::move(child));
        continue;
      }

      auto wrapper = makeInlineReadFromCommonBufferWrapper(*producer, wrapped + 1);
      wrapper->common_buffer_producer = child.get();
      auto wrapper_shared = std::shared_ptr<PlanNode>(std::move(wrapper));
      wrapper_shared->addSharedChild(std::move(child));
      parent.addSharedChild(std::move(wrapper_shared));
      replaced = true;
    }

    if (replaced) {
      ++wrapped;
      PLOGD << "Inserted synthetic inline ReadFromCommonBuffer wrapper for producer node_id="
            << producer->node_id
            << " consumers=" << consumer_count_by_producer.at(producer);
    }
  }
  return wrapped > 0;
}

void buildResolvedJsonPlanTreeFromExplainJson(const json& explain_json, ExplainCtx& ctx) {
  if (!explain_json.is_array() || explain_json.empty() || !explain_json[0].contains("Plan")) {
    throw ExplainException("Cannot build resolved JSON plan tree: missing top-level Plan");
  }
  ctx.resolved_json_plan_root = buildResolvedPlanNodeTree(explain_json[0]["Plan"]);
  if (!ctx.resolved_json_plan_root) {
    throw ExplainException("Failed to build resolved JSON plan tree");
  }
  // PlanNode pass pipeline starts here. Structural rewrites happen before
  // lowering to canonical sql::explain::Node.
  (void)applyStrictnessJoinTypePlanNodes(*ctx.resolved_json_plan_root);
  (void)flattenCreatingSetWrappersPlanNodes(*ctx.resolved_json_plan_root);
  (void)flipJoinChildrenPlanNodes(*ctx.resolved_json_plan_root);
  (void)pruneBroadcastPlanNodes(*ctx.resolved_json_plan_root);
  (void)insertInlineMaterialisedReadPlanNodes(*ctx.resolved_json_plan_root);
  indexPlanNodeTreeById(*ctx.resolved_json_plan_root, ctx.resolved_json_plan_nodes_by_id);
}

std::string getOrFetchClickHouseExplainJson(Connection& connection,
                                            const std::string_view statement,
                                            const std::string_view artifact_name) {
  return fetchClickHouseExplainJson(
    connection,
    statement,
    artifact_name,
    connection.getArtefact(artifact_name, "json"));
}

std::string getOrFetchClickHouseExplainAst(Connection& connection,
                                           const std::string_view statement,
                                           const std::string_view artifact_name) {
  return fetchClickHouseExplainAst(
    connection,
    statement,
    artifact_name,
    connection.getArtefact(artifact_name, "ast"));
}

std::string getOrFetchClickHouseExplainQueryTree(Connection& connection,
                                                 const std::string_view statement,
                                                 const std::string_view artifact_name) {
  return fetchClickHouseExplainQueryTree(
    connection,
    statement,
    artifact_name,
    connection.getArtefact(artifact_name, "query_tree"));
}

std::unique_ptr<Plan> Connection::explain(const std::string_view statement, std::optional<std::string_view> name) {
  const std::string artifact_name = name.has_value() ? std::string(*name) : std::to_string(std::hash<std::string_view>{}(statement));
  const auto string_explain = getOrFetchClickHouseExplainJson(*this, statement, artifact_name);
  const auto ast_explain = getOrFetchClickHouseExplainAst(*this, statement, artifact_name);
  const auto query_tree = getOrFetchClickHouseExplainQueryTree(*this, statement, artifact_name);

  ExplainCtx ctx;
  const auto guessed_sets = guessSetsFromAst(ast_explain, &ctx.dialect);

  for (const auto& [key, normalized_set] : guessed_sets) {
    if (ctx.filterSets.contains(key)) {
      continue;
    }
    ctx.addFilterSet(key, normalized_set);
  }
  for (const auto& exists_relation : guessCorrelatedExistsRelationsFromQueryTree(query_tree)) {
    ctx.correlated_exists_relations.push_back(CorrelatedExistsRelation{
      .left_table = exists_relation.left_table,
      .left_column = exists_relation.left_column,
      .right_table = exists_relation.right_table,
      .right_column = exists_relation.right_column,
    });
  }
  auto json_explain = json::parse(string_explain);
  if (json_explain.is_array() && !json_explain.empty() &&
      json_explain[0].contains("Plan")) {
    buildResolvedJsonPlanTreeFromExplainJson(json_explain, ctx);
    // Alias and synthetic-qualifier collection is deferred to a later phase.
  }

  auto plan = buildExplainPlan(json_explain, ctx);
  const auto* skip_actuals_env = std::getenv("DBPROVE_SKIP_ACTUALS");
  const bool skip_actuals = skip_actuals_env != nullptr &&
                            std::string_view(skip_actuals_env) == "1";
  if (!skip_actuals) {
    plan->fixActuals(*this);
  }
  return plan;
}
}
