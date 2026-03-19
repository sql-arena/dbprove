#include "expression_node.h"

#include <algorithm>
#include <cctype>
#include <array>
#include <set>
#include <stdexcept>
#include <string_view>

#include <dbprove/common/string.h>
#include "literals.h"
#include "sql.h"

namespace sql::clickhouse {
namespace {
bool isSyntheticAliasNode(const ExpressionNode& node);
const ExpressionNode* resolveRealAliasBoundaryInReferenceChain(const ExpressionNode& node);
std::string realAliasToken(const ExpressionNode& node);
std::vector<const ExpressionNode*> snapshotChildren(const ExpressionNode& node) {
  std::vector<const ExpressionNode*> out;
  out.reserve(node.childCount());
  for (const auto* child : node.children()) {
    if (child != nullptr) {
      out.push_back(child);
    }
  }
  return out;
}

bool isBooleanConnectorFn(const std::string& fn_name) {
  const auto upper = to_upper(trim_string(fn_name));
  return upper == "AND" || upper == "OR";
}

bool isCastFn(const std::string& fn_name) {
  const auto upper = to_upper(trim_string(fn_name));
  return upper == "_CAST" || upper == "CAST";
}

bool isNoopFn(const std::string& fn_name) {
  const auto upper = to_upper(trim_string(fn_name));
  return upper == "TONULLABLE";
}

bool isLikeFn(const std::string& fn_name) {
  const auto upper = to_upper(trim_string(fn_name));
  return upper == "LIKE" || upper == "ILIKE";
}

bool containsExistsCallToken(const std::string& text) {
  auto upper = to_upper(trim_string(text));
  if (upper.empty()) {
    return false;
  }

  constexpr std::string_view exists_token = "EXISTS";
  size_t pos = 0;
  while ((pos = upper.find(exists_token, pos)) != std::string::npos) {
    const bool left_boundary =
        pos == 0 || (!std::isalnum(static_cast<unsigned char>(upper[pos - 1])) && upper[pos - 1] != '_');
    if (!left_boundary) {
      pos += exists_token.size();
      continue;
    }

    size_t i = pos + exists_token.size();
    while (i < upper.size() && std::isspace(static_cast<unsigned char>(upper[i])) != 0) {
      ++i;
    }
    if (i < upper.size() && upper[i] == '(') {
      return true;
    }
    pos += exists_token.size();
  }
  return false;
}

std::string renderLikePredicate(const std::string& fn_name, const std::vector<std::string>& args) {
  if (args.size() != 2) {
    throw std::runtime_error("LIKE/ILIKE ExpressionNode must have exactly two arguments");
  }
  return args[0] + " " + to_upper(trim_string(fn_name)) + " " + args[1];
}

std::string stripTopLevelCastText(std::string expression) {
  auto unwrap_once = [](const std::string& input) -> std::string {
    const auto expr = cleanExpression(trim_string(input));
    const auto upper = to_upper(expr);
    size_t prefix_len = 0;
    if (upper.starts_with("_CAST(")) {
      prefix_len = 6;
    } else if (upper.starts_with("CAST(")) {
      prefix_len = 5;
    } else {
      return expr;
    }
    if (expr.size() <= prefix_len || expr.back() != ')') {
      return expr;
    }

    const auto inside = expr.substr(prefix_len, expr.size() - prefix_len - 1);
    int depth = 0;
    bool in_single_quote = false;
    for (size_t i = 0; i < inside.size(); ++i) {
      const auto c = inside[i];
      if (c == '\'') {
        if (in_single_quote && i + 1 < inside.size() && inside[i + 1] == '\'') {
          ++i;
          continue;
        }
        in_single_quote = !in_single_quote;
        continue;
      }
      if (in_single_quote) {
        continue;
      }
      if (c == '(') {
        ++depth;
      } else if (c == ')') {
        if (depth > 0) {
          --depth;
        }
      } else if (c == ',' && depth == 0) {
        return cleanExpression(trim_string(inside.substr(0, i)));
      }
    }
    return expr;
  };

  while (true) {
    const auto unwrapped = unwrap_once(expression);
    if (to_upper(cleanExpression(unwrapped)) == to_upper(cleanExpression(expression))) {
      return cleanExpression(unwrapped);
    }
    expression = unwrapped;
  }
}

std::string renderBooleanConnector(const std::string& fn_name, const std::vector<std::string>& args) {
  if (args.empty()) {
    return trim_string(fn_name) + "()";
  }
  if (args.size() == 1) {
    return args.front();
  }
  const auto op = to_upper(trim_string(fn_name));
  std::string rendered = "(";
  for (size_t i = 0; i < args.size(); ++i) {
    if (i > 0) {
      rendered += " ";
      rendered += op;
      rendered += " ";
    }
    rendered += args[i];
  }
  rendered += ")";
  return rendered;
}

std::string unqualifiedColumnToken(std::string token) {
  token = cleanExpression(trim_string(std::move(token)));
  const auto dot = token.find_last_of('.');
  if (dot == std::string::npos || dot + 1 >= token.size()) {
    return token;
  }
  return token.substr(dot + 1);
}

bool isSyntheticNestedOutputName(const std::string& token) {
  const auto cleaned = to_upper(cleanExpression(trim_string(token)));
  return cleaned.contains("__TABLE") && cleaned.find('.') != std::string::npos;
}

std::string renderExpressionNodeSqlQualifiedInternal(
    const ExpressionNode& node,
    const std::map<std::string, std::string>& plan_alias_by_node_id) {
  if (node.kind == ExpressionNode::Kind::REFERENCE) {
    std::string direct_alias;
    if (!node.linked_child_node_id.empty() &&
        plan_alias_by_node_id.contains(node.linked_child_node_id)) {
      direct_alias = plan_alias_by_node_id.at(node.linked_child_node_id);
    }

    auto qualify_with_direct_alias = [&](std::string token) -> std::string {
      token = cleanExpression(std::move(token));
      if (token.empty() || direct_alias.empty()) {
        return token;
      }
      if (token.find('.') != std::string::npos) {
        return token;
      }
      return cleanExpression(direct_alias + "." + unqualifiedColumnToken(std::move(token)));
    };

    if (node.references != nullptr) {
      return qualify_with_direct_alias(
          renderExpressionNodeSqlQualifiedInternal(*node.references, plan_alias_by_node_id));
    }
    if (node.childCount() == 1 && node.firstChild() != nullptr) {
      return qualify_with_direct_alias(
          renderExpressionNodeSqlQualifiedInternal(*node.firstChild(), plan_alias_by_node_id));
    }
    if (node.linked_child_output_root != nullptr) {
      return qualify_with_direct_alias(
          renderExpressionNodeSqlQualifiedInternal(*node.linked_child_output_root, plan_alias_by_node_id));
    }

    if (!node.linked_child_node_id.empty() &&
        !node.linked_child_output_name.empty() &&
        plan_alias_by_node_id.contains(node.linked_child_node_id) &&
        !isSyntheticNestedOutputName(node.linked_child_output_name)) {
      const auto qualified = plan_alias_by_node_id.at(node.linked_child_node_id) + "." +
                             unqualifiedColumnToken(node.linked_child_output_name);
      return cleanExpression(qualified);
    }

    std::string plan_node_id = node.linked_child_node_id;
    if (plan_node_id.empty() && node.linked_child_output_root != nullptr) {
      plan_node_id = node.linked_child_output_root->plan_node_id;
    }
    if (plan_node_id.empty()) {
      plan_node_id = node.plan_node_id;
    }

    std::string column_name = node.linked_child_output_name;
    if (column_name.empty() && node.linked_child_output_root != nullptr) {
      column_name = node.linked_child_output_root->output_name;
    }
    if (column_name.empty()) {
      column_name = !node.output_name.empty() ? node.output_name
                  : !node.source_name.empty() ? node.source_name
                  : node.result_name;
    }
    column_name = unqualifiedColumnToken(column_name);

    if (!plan_node_id.empty() &&
        !column_name.empty() &&
        plan_alias_by_node_id.contains(plan_node_id)) {
      return cleanExpression(plan_alias_by_node_id.at(plan_node_id) + "." + column_name);
    }

    if (!node.source_name.empty()) {
      return stripTopLevelCastText(node.source_name);
    }
    throw std::runtime_error("Cannot render REFERENCE ExpressionNode without child or child output binding");
  }

  if (node.kind == ExpressionNode::Kind::ALIAS && node.childCount() == 1 && node.firstChild() != nullptr) {
    return renderExpressionNodeSqlQualifiedInternal(*node.firstChild(), plan_alias_by_node_id);
  }

  if (node.kind == ExpressionNode::Kind::FUNCTION) {
    std::vector<std::string> args;
    args.reserve(node.childCount());
    const auto children = snapshotChildren(node);
    for (const auto* child : children) {
      args.push_back(renderExpressionNodeSqlQualifiedInternal(*child, plan_alias_by_node_id));
    }
    const auto fn_name = trim_string(node.function_name);
    if (fn_name.empty()) {
      throw std::runtime_error("Cannot render FUNCTION ExpressionNode without function name");
    }
    if ((isCastFn(fn_name) || isNoopFn(fn_name)) && !args.empty()) {
      return args.front();
    }
    if (isLikeFn(fn_name)) {
      return renderLikePredicate(fn_name, args);
    }
    if (isBooleanConnectorFn(fn_name)) {
      return renderBooleanConnector(fn_name, args);
    }
    std::string rendered = fn_name + "(";
    for (size_t i = 0; i < args.size(); ++i) {
      if (i > 0) {
        rendered += ", ";
      }
      rendered += args[i];
    }
    rendered += ")";
    return rendered;
  }

  if (node.childCount() == 0) {
    if (node.leaf_binding == ExpressionNode::LeafBinding::CHILD_OUTPUT &&
        !node.linked_child_output_name.empty()) {
      return node.linked_child_output_name;
    }
    if (!node.source_name.empty()) {
      return stripTopLevelCastText(node.source_name);
    }
    throw std::runtime_error("Cannot render leaf ExpressionNode without bound child output or source name");
  }

  throw std::runtime_error("Cannot render non-leaf non-function ExpressionNode");
}

std::string renderExpressionNodeSqlInternal(const ExpressionNode& node) {
  if (node.kind == ExpressionNode::Kind::REFERENCE) {
    if (node.references != nullptr) {
      if (const auto* alias_target = resolveRealAliasBoundaryInReferenceChain(*node.references); alias_target != nullptr) {
        const auto alias_token = realAliasToken(*alias_target);
        if (!alias_token.empty()) {
          return alias_token;
        }
      }
      return renderExpressionNodeSqlInternal(*node.references);
    }
    if (node.childCount() == 1 && node.firstChild() != nullptr) {
      if (const auto* alias_target = resolveRealAliasBoundaryInReferenceChain(*node.firstChild()); alias_target != nullptr) {
        const auto alias_token = realAliasToken(*alias_target);
        if (!alias_token.empty()) {
          return alias_token;
        }
      }
      return renderExpressionNodeSqlInternal(*node.firstChild());
    }
    if (node.leaf_binding == ExpressionNode::LeafBinding::CHILD_OUTPUT &&
        !node.linked_child_output_name.empty()) {
      return node.linked_child_output_name;
    }
    if (!node.source_name.empty()) {
      return node.source_name;
    }
    throw std::runtime_error("Cannot render REFERENCE ExpressionNode without child or child output binding");
  }

  if (node.kind == ExpressionNode::Kind::ALIAS && node.childCount() == 1 && node.firstChild() != nullptr) {
    return renderExpressionNodeSqlInternal(*node.firstChild());
  }

  if (node.kind == ExpressionNode::Kind::FUNCTION) {
    std::vector<std::string> args;
    args.reserve(node.childCount());
    const auto children = snapshotChildren(node);
    for (const auto* child : children) {
      args.push_back(renderExpressionNodeSqlInternal(*child));
    }
    const auto fn_name = trim_string(node.function_name);
    if (fn_name.empty()) {
      throw std::runtime_error("Cannot render FUNCTION ExpressionNode without function name");
    }
    if ((isCastFn(fn_name) || isNoopFn(fn_name)) && !args.empty()) {
      return args.front();
    }
    if (isLikeFn(fn_name)) {
      return renderLikePredicate(fn_name, args);
    }
    if (isBooleanConnectorFn(fn_name)) {
      return renderBooleanConnector(fn_name, args);
    }
    std::string rendered = fn_name + "(";
    for (size_t i = 0; i < args.size(); ++i) {
      if (i > 0) {
        rendered += ", ";
      }
      rendered += args[i];
    }
    rendered += ")";
    return rendered;
  }

  if (node.childCount() == 0) {
    if (node.leaf_binding == ExpressionNode::LeafBinding::CHILD_OUTPUT &&
        !node.linked_child_output_name.empty()) {
      return node.linked_child_output_name;
    }
    if (!node.source_name.empty()) {
      return node.source_name;
    }
    throw std::runtime_error("Cannot render leaf ExpressionNode without bound child output or source name");
  }

  throw std::runtime_error("Cannot render non-leaf non-function ExpressionNode");
}

bool isSyntheticTableAlias(const std::string& value) {
  const auto trimmed = trim_string(value);
  if (trimmed.empty()) {
    return false;
  }
  // ClickHouse internal alias wrappers can carry full expression text as alias names.
  // Treat these as synthetic so rendering keeps unfolding to semantic children.
  if (trimmed.find('(') != std::string::npos && trimmed.find(')') != std::string::npos) {
    return true;
  }
  const auto dot = trimmed.find('.');
  const auto qualifier = dot == std::string::npos ? trimmed : trimmed.substr(0, dot);
  const auto qualifier_upper = to_upper(qualifier);
  if (!qualifier_upper.starts_with("__TABLE") || qualifier.size() <= 7) {
    return false;
  }
  return std::all_of(qualifier.begin() + 7, qualifier.end(), [](const char c) {
    return std::isdigit(static_cast<unsigned char>(c)) != 0;
  });
}

bool isSyntheticAliasNode(const ExpressionNode& node) {
  if (node.kind != ExpressionNode::Kind::ALIAS) {
    return false;
  }
  return isSyntheticTableAlias(node.result_name) ||
         isSyntheticTableAlias(node.output_name) ||
         isSyntheticTableAlias(node.source_name);
}

const ExpressionNode* resolveRealAliasBoundaryInReferenceChain(const ExpressionNode& node) {
  std::set<const ExpressionNode*> visited;
  const ExpressionNode* current = &node;
  while (current != nullptr) {
    if (visited.contains(current)) {
      return nullptr;
    }
    visited.insert(current);

    if (current->isRealAliased()) {
      return current;
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
      return nullptr;
    }

    if (isSyntheticAliasNode(*current) && current->childCount() == 1 && current->firstChild() != nullptr) {
      current = current->firstChild();
      continue;
    }

    return nullptr;
  }
  return nullptr;
}

std::string realAliasToken(const ExpressionNode& node) {
  if (!node.alias_user.empty()) {
    return node.alias_user;
  }
  if (!node.output_name.empty()) {
    return node.output_name;
  }
  if (!node.result_name.empty()) {
    return node.result_name;
  }
  return "";
}

std::string renderExpressionNodeUserInternal(const ExpressionNode& node) {
  if (node.kind == ExpressionNode::Kind::FUNCTION) {
    const auto fn_name = trim_string(node.function_name);
    if (fn_name.empty()) {
      throw std::runtime_error("Cannot render FUNCTION ExpressionNode without function name");
    }
    size_t arg_index = 0;
    const auto children = snapshotChildren(node);
    std::vector<std::string> args;
    args.reserve(children.size());
    std::string rendered = fn_name + "(";
    for (const auto* child : children) {
      if (arg_index++ > 0) {
        rendered += ", ";
      }
      const auto child_rendered = renderExpressionNodeUserInternal(*child);
      rendered += child_rendered;
      args.push_back(child_rendered);
    }
    rendered += ")";
    if ((isCastFn(fn_name) || isNoopFn(fn_name)) && !args.empty()) {
      return args.front();
    }
    if (isLikeFn(fn_name)) {
      return renderLikePredicate(fn_name, args);
    }
    if (isBooleanConnectorFn(fn_name)) {
      return renderBooleanConnector(fn_name, args);
    }
    return rendered;
  }

  if (node.kind == ExpressionNode::Kind::ALIAS) {
    if (isSyntheticAliasNode(node) && node.childCount() == 1 && node.firstChild() != nullptr) {
      return renderExpressionNodeUserInternal(*node.firstChild());
    }
    if (!node.output_name.empty()) {
      return node.output_name;
    }
    if (!node.result_name.empty()) {
      return node.result_name;
    }
    if (node.childCount() == 1 && node.firstChild() != nullptr) {
      return renderExpressionNodeUserInternal(*node.firstChild());
    }
  }

  if (node.kind == ExpressionNode::Kind::REFERENCE) {
    if (node.references != nullptr) {
      if (const auto* alias_target = resolveRealAliasBoundaryInReferenceChain(*node.references); alias_target != nullptr) {
        const auto alias_token = realAliasToken(*alias_target);
        if (!alias_token.empty()) {
          return alias_token;
        }
      }
      return renderExpressionNodeUserInternal(*node.references);
    }
    const ExpressionNode* current = &node;
    while (current != nullptr && current->childCount() == 1 && current->firstChild() != nullptr) {
      if (current->kind != ExpressionNode::Kind::REFERENCE &&
          !isSyntheticAliasNode(*current)) {
        break;
      }
      current = current->firstChild();
    }
    if (current != nullptr) {
      if (current->isRealAliased()) {
        const auto alias_token = realAliasToken(*current);
        if (!alias_token.empty()) {
          return alias_token;
        }
      }
      if (current->kind == ExpressionNode::Kind::FUNCTION) {
        return renderExpressionNodeUserInternal(*current);
      }
      if (current->leaf_binding == ExpressionNode::LeafBinding::CHILD_OUTPUT &&
          !current->linked_child_output_name.empty()) {
        if (current->linked_child_output_root != nullptr) {
          return renderExpressionNodeUserInternal(*current->linked_child_output_root);
        }
        return current->linked_child_output_name;
      }
      if (!current->source_name.empty()) {
        return current->source_name;
      }
      if (!current->result_name.empty()) {
        return current->result_name;
      }
    }
  }
  if (!node.source_name.empty()) {
    return stripTopLevelCastText(node.source_name);
  }
  if (!node.result_name.empty()) {
    return node.result_name;
  }
  return renderExpressionNodeSqlInternal(node);
}
} // namespace

bool ExpressionNode::isExists() const {
  if (kind == ExpressionNode::Kind::FUNCTION &&
      to_upper(trim_string(function_name)) == "EXISTS") {
    return true;
  }
  for (const auto* candidate : std::array<const std::string*, 6>{
         &function_name, &expression, &result_name, &source_name, &output_name, &linked_child_output_name}) {
    if (candidate != nullptr && containsExistsCallToken(*candidate)) {
      return true;
    }
  }
  return false;
}

bool ExpressionNode::isRealAliased() const {
  if (!alias_user.empty()) {
    return true;
  }
  if (kind != ExpressionNode::Kind::ALIAS) {
    return false;
  }
  return !isSyntheticAliasNode(*this);
}

std::string ExpressionNode::renderExecutableSql() const {
  return renderSql();
}

void ExpressionNode::setExpression(std::string value) {
  expression = stripClickHouseTypedLiterals(std::move(value));
}

std::string ExpressionNode::renderSql() const {
  return renderExpressionNodeSqlInternal(*this);
}

std::string ExpressionNode::renderSql(const std::map<std::string, std::string>& plan_alias_by_node_id) const {
  return renderExpressionNodeSqlQualifiedInternal(*this, plan_alias_by_node_id);
}

std::string ExpressionNode::renderUser() const {
  auto rendered = renderExpressionNodeUserInternal(*this);
  if (!alias_user.empty()) {
    rendered += " AS " + alias_user;
  }
  return rendered;
}

std::vector<std::string> ExpressionNode::renderExecutableSqlList(const std::vector<ExpressionNode>& output_expressions) {
  std::vector<std::string> rendered;
  rendered.reserve(output_expressions.size());
  for (const auto& expression_root : output_expressions) {
    if (expression_root.output_name.empty()) {
      throw std::runtime_error("Expression output root missing output_name");
    }
    auto expression_sql = expression_root.renderExecutableSql();
    if (expression_sql.empty()) {
      throw std::runtime_error("Expression output rendered empty SQL");
    }
    const auto output_name = expression_root.output_name;
    if (!output_name.empty() && to_upper(expression_sql) != to_upper(output_name)) {
      expression_sql += " AS " + output_name;
    }
    rendered.push_back(std::move(expression_sql));
  }
  return rendered;
}

} // namespace sql::clickhouse
