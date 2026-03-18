#include "plan_node.h"

#include <cctype>
#include <regex>
#include <set>
#include <sstream>
#include <stdexcept>

#include <dbprove/common/string.h>
#include <plog/Log.h>
#include "literals.h"
#include "sql.h"

namespace sql::clickhouse {
namespace {
using nlohmann::json;

struct ChildOutputCandidate {
  std::string child_node_id;
  std::string output_name;
  ExpressionNode* root = nullptr;
};

struct ParsedActionNode {
  int64_t internal_id = -1;
  int64_t result_slot = -1;
  int64_t input_slot = -1;
  std::string node_type;
  std::string result_name;
  std::string function_name;
  std::string column_name;
  std::vector<int64_t> arguments;
};

struct ParsedActionProgram {
  std::vector<int64_t> topological_order;
  std::map<int64_t, ParsedActionNode> nodes_by_internal_id;
  std::unordered_map<int64_t, int64_t> latest_internal_id_by_result_slot;
};

bool expressionTreeContainsExistsRoot(const ExpressionNode& root);
std::string sanitizeIdentifier(std::string token);
bool isSimpleConstantLiteral(const std::string& value);

bool isKeywordToken(const std::string& token) {
  static const std::set<std::string> keywords = {
    "AND", "OR", "NOT", "IN", "NOTIN", "TRUE", "FALSE", "NULL", "EXISTS"
  };
  return keywords.contains(to_upper(trim_string(token)));
}

std::vector<std::string> splitTopLevelByDelimiter(const std::string& text, const char delimiter) {
  std::vector<std::string> out;
  int depth = 0;
  size_t start = 0;
  for (size_t i = 0; i < text.size(); ++i) {
    if (text[i] == '(') {
      ++depth;
      continue;
    }
    if (text[i] == ')') {
      --depth;
      continue;
    }
    if (text[i] == delimiter && depth == 0) {
      out.push_back(trim_string(text.substr(start, i - start)));
      start = i + 1;
    }
  }
  out.push_back(trim_string(text.substr(start)));
  return out;
}

std::vector<std::string> splitTopLevelConjunctions(std::string text) {
  std::vector<std::string> out;
  text = trim_string(std::move(text));
  if (text.empty()) {
    return out;
  }
  int depth = 0;
  size_t start = 0;
  for (size_t i = 0; i + 2 < text.size(); ++i) {
    const char c = text[i];
    if (c == '(') {
      ++depth;
      continue;
    }
    if (c == ')') {
      --depth;
      continue;
    }
    if (depth != 0) {
      continue;
    }
    if (to_upper(std::string(1, text[i])) != "A") {
      continue;
    }
    if (to_upper(text.substr(i, 3)) != "AND") {
      continue;
    }
    const bool left_ok = i == 0 || std::isspace(static_cast<unsigned char>(text[i - 1])) != 0;
    const bool right_ok = (i + 3) >= text.size() || std::isspace(static_cast<unsigned char>(text[i + 3])) != 0;
    if (!left_ok || !right_ok) {
      continue;
    }
    out.push_back(trim_string(text.substr(start, i - start)));
    start = i + 3;
    i += 2;
  }
  out.push_back(trim_string(text.substr(start)));
  return out;
}

std::string stripOuterParens(std::string text) {
  text = trim_string(std::move(text));
  while (text.size() >= 2 && text.front() == '(' && text.back() == ')') {
    int depth = 0;
    bool wraps_entire = true;
    for (size_t i = 0; i < text.size(); ++i) {
      if (text[i] == '(') {
        ++depth;
      } else if (text[i] == ')') {
        --depth;
      }
      if (depth == 0 && i + 1 < text.size()) {
        wraps_entire = false;
        break;
      }
    }
    if (!wraps_entire) {
      break;
    }
    text = trim_string(text.substr(1, text.size() - 2));
  }
  return text;
}

std::unique_ptr<ExpressionNode> makeReferenceLeaf(const std::string& node_id, const std::string& expression) {
  auto ref = std::make_unique<ExpressionNode>();
  ref->kind = ExpressionNode::Kind::REFERENCE;
  ref->plan_node_id = node_id;
  ref->result_name = expression;
  ref->source_name = expression;
  ref->output_name = expression;
  ref->setExpression(expression);
  return ref;
}

std::unique_ptr<ExpressionNode> parseClauseExpression(const std::string& node_id, const std::string& text) {
  auto expr = stripOuterParens(text);
  const auto tuple_parts = splitTopLevelByDelimiter(expr, ',');
  if (tuple_parts.size() > 1) {
    auto tuple_node = std::make_unique<ExpressionNode>();
    tuple_node->kind = ExpressionNode::Kind::FUNCTION;
    tuple_node->plan_node_id = node_id;
    tuple_node->function_name = "tuple";
    tuple_node->result_name = expr;
    tuple_node->setExpression(expr);
    for (const auto& part : tuple_parts) {
      if (part.empty()) {
        continue;
      }
      tuple_node->addChild(parseClauseExpression(node_id, part));
    }
    return tuple_node;
  }
  return makeReferenceLeaf(node_id, expr);
}

std::optional<size_t> findTopLevelEqualsPosition(const std::string& text) {
  int depth = 0;
  for (size_t i = 0; i < text.size(); ++i) {
    const auto c = text[i];
    if (c == '(') {
      ++depth;
      continue;
    }
    if (c == ')') {
      --depth;
      continue;
    }
    if (depth == 0 && c == '=') {
      return i;
    }
  }
  return std::nullopt;
}

void buildEqualsClause(ExpressionNode& out,
                       const std::string& node_id,
                       const std::string& predicate) {
  const auto normalized = stripOuterParens(predicate);
  if (normalized.empty()) {
    throw std::runtime_error("Invalid empty join clause predicate in node '" + node_id + "'");
  }
  const auto equals_pos = findTopLevelEqualsPosition(normalized);
  if (!equals_pos.has_value()) {
    throw std::runtime_error("Unsupported join clause predicate in node '" + node_id + "': " + predicate);
  }

  auto lhs = trim_string(normalized.substr(0, *equals_pos));
  auto rhs = trim_string(normalized.substr(*equals_pos + 1));
  if (lhs.empty() || rhs.empty()) {
    throw std::runtime_error("Invalid join clause predicate in node '" + node_id + "': " + predicate);
  }

  out.kind = ExpressionNode::Kind::FUNCTION;
  out.plan_node_id = node_id;
  out.function_name = "equals";
  out.result_name = normalized;
  out.setExpression(normalized);
  out.addChild(parseClauseExpression(node_id, lhs));
  out.addChild(parseClauseExpression(node_id, rhs));
}

std::vector<ExpressionNode> parseClausesToExpressions(const std::string& node_id, const std::string& unresolved_clauses) {
  auto raw = trim_string(unresolved_clauses);
  if (raw.empty()) {
    return {};
  }
  if (raw.front() == '[' && raw.back() == ']') {
    raw = trim_string(raw.substr(1, raw.size() - 2));
  }
  if (raw.empty()) {
    return {};
  }

  std::vector<std::string> predicates;
  for (auto& segment : splitTopLevelConjunctions(raw)) {
    if (segment.empty()) {
      continue;
    }
    auto comma_split = splitTopLevelByDelimiter(segment, ',');
    if (comma_split.size() > 1) {
      for (auto& comma_term : comma_split) {
        if (!comma_term.empty()) {
          predicates.push_back(std::move(comma_term));
        }
      }
    } else {
      predicates.push_back(std::move(segment));
    }
  }

  std::vector<ExpressionNode> clauses(predicates.size());
  for (size_t i = 0; i < predicates.size(); ++i) {
    buildEqualsClause(clauses[i], node_id, predicates[i]);
  }

  return clauses;
}

std::vector<PlanNodeHeader> parseHeaders(const json& node_json) {
  std::vector<PlanNodeHeader> headers;
  if (!node_json.contains("Header") || !node_json["Header"].is_array()) {
    return headers;
  }
  for (const auto& entry : node_json["Header"]) {
    if (!entry.contains("Name") || !entry["Name"].is_string()) {
      continue;
    }
    headers.emplace_back();
    headers.back().name = entry["Name"].get<std::string>();
    headers.back().type = entry.contains("Type") && entry["Type"].is_string()
                            ? entry["Type"].get<std::string>()
                            : "";
  }
  return headers;
}

std::vector<std::string> parseKeys(const json& node_json) {
  std::vector<std::string> keys;
  if (!node_json.contains("Keys") || !node_json["Keys"].is_array()) {
    return keys;
  }
  for (const auto& key : node_json["Keys"]) {
    if (!key.is_string()) {
      continue;
    }
    keys.push_back(key.get<std::string>());
  }
  return keys;
}

std::vector<std::string> parseFilterColumns(const json& node_json) {
  std::vector<std::string> filter_columns;
  if (!node_json.contains("Filter Column")) {
    return filter_columns;
  }
  const auto& raw = node_json["Filter Column"];
  if (raw.is_string()) {
    filter_columns.push_back(raw.get<std::string>());
    return filter_columns;
  }
  if (!raw.is_array()) {
    return filter_columns;
  }
  for (const auto& entry : raw) {
    if (!entry.is_string()) {
      continue;
    }
    filter_columns.push_back(entry.get<std::string>());
  }
  return filter_columns;
}

std::map<int64_t, std::string> parseInputNamesBySlot(const json& node_json) {
  std::map<int64_t, std::string> names_by_slot;
  if (!node_json.contains("Expression") || !node_json["Expression"].is_object()) {
    return names_by_slot;
  }
  const auto& expr_json = node_json["Expression"];
  if (!expr_json.contains("Inputs") || !expr_json["Inputs"].is_array()) {
    return names_by_slot;
  }
  const auto& inputs = expr_json["Inputs"];
  for (size_t i = 0; i < inputs.size(); ++i) {
    if (!inputs[i].is_object() || !inputs[i].contains("Name") || !inputs[i]["Name"].is_string()) {
      continue;
    }
    names_by_slot[static_cast<int64_t>(i)] = inputs[i]["Name"].get<std::string>();
  }
  return names_by_slot;
}

std::pair<std::vector<std::string>, std::vector<bool>> parseSortColumns(const json& node_json) {
  std::vector<std::string> columns;
  std::vector<bool> ascending;
  if (!node_json.contains("Sort Description") || !node_json["Sort Description"].is_array()) {
    return {columns, ascending};
  }
  for (const auto& sort_entry : node_json["Sort Description"]) {
    if (!sort_entry.contains("Column") || !sort_entry["Column"].is_string()) {
      continue;
    }
    columns.push_back(sort_entry["Column"].get<std::string>());
    const bool is_ascending = sort_entry.contains("Ascending") && sort_entry["Ascending"].is_boolean()
                                ? sort_entry["Ascending"].get<bool>()
                                : true;
    ascending.push_back(is_ascending);
  }
  return {columns, ascending};
}

std::optional<json> parsePrewhereFilterExpressionJson(const json& node_json) {
  if (!node_json.contains("Prewhere info") || !node_json["Prewhere info"].is_object()) {
    return std::nullopt;
  }
  const auto& prewhere_info = node_json["Prewhere info"];
  if (!prewhere_info.contains("Prewhere filter") || !prewhere_info["Prewhere filter"].is_object()) {
    return std::nullopt;
  }
  const auto& prewhere_filter = prewhere_info["Prewhere filter"];
  if (!prewhere_filter.contains("Prewhere filter expression") ||
      !prewhere_filter["Prewhere filter expression"].is_object()) {
    return std::nullopt;
  }
  return json(prewhere_filter["Prewhere filter expression"]);
}

ExpressionNode::Kind expressionNodeKindFromActionType(const std::string& node_type) {
  const auto node_type_upper = to_upper(trim_string(node_type));
  if (node_type_upper == "FUNCTION") {
    return ExpressionNode::Kind::FUNCTION;
  }
  if (node_type_upper == "ALIAS") {
    return ExpressionNode::Kind::ALIAS;
  }
  if (node_type_upper == "COLUMN") {
    return ExpressionNode::Kind::COLUMN;
  }
  if (node_type_upper == "INPUT") {
    return ExpressionNode::Kind::INPUT;
  }
  return ExpressionNode::Kind::UNKNOWN;
}

std::optional<ParsedActionProgram> parseActionProgram(const json& actions_json) {
  if (!actions_json.is_array() || actions_json.empty()) {
    return std::nullopt;
  }

  ParsedActionProgram program;
  program.topological_order.reserve(actions_json.size());

  int64_t next_internal_id = 1;
  for (const auto& action_json : actions_json) {
    if (!action_json.contains("Result") || !action_json["Result"].is_number_integer()) {
      continue;
    }

    ParsedActionNode parsed_action;
    parsed_action.internal_id = next_internal_id++;
    parsed_action.result_slot = action_json["Result"].get<int64_t>();
    parsed_action.node_type = action_json.value("Node Type", "");
    parsed_action.result_name = action_json.value("Result Name", "");
    parsed_action.function_name = action_json.value("Function", "");
    parsed_action.column_name = action_json.value("Column", "");

    if (action_json.contains("Arguments") && action_json["Arguments"].is_array()) {
      if (to_upper(trim_string(parsed_action.node_type)) == "INPUT") {
        const auto& args_json = action_json["Arguments"];
        if (!args_json.empty() && args_json[0].is_number_integer()) {
          parsed_action.input_slot = args_json[0].get<int64_t>();
        }
      } else {
        for (const auto& argument_json : action_json["Arguments"]) {
          if (!argument_json.is_number_integer()) {
            continue;
          }
          const auto argument_slot = argument_json.get<int64_t>();
          if (!program.latest_internal_id_by_result_slot.contains(argument_slot)) {
            continue;
          }
          parsed_action.arguments.push_back(program.latest_internal_id_by_result_slot.at(argument_slot));
        }
      }
    }

    program.topological_order.push_back(parsed_action.internal_id);
    program.nodes_by_internal_id[parsed_action.internal_id] = parsed_action;
    program.latest_internal_id_by_result_slot[parsed_action.result_slot] = parsed_action.internal_id;
  }

  if (program.nodes_by_internal_id.empty()) {
    return std::nullopt;
  }
  return program;
}

void buildExpressionNodeFromActionTreeNoRender(ExpressionNode& out,
                                               const ParsedActionProgram& action_program,
                                               const int64_t internal_id,
                                               const std::string& plan_node_id) {
  if (!action_program.nodes_by_internal_id.contains(internal_id)) {
    return;
  }
  const auto& action = action_program.nodes_by_internal_id.at(internal_id);
  out.plan_node_id = plan_node_id;
  out.result_id = internal_id;
  out.result_slot = action.result_slot;
  out.input_slot = action.input_slot;
  out.kind = expressionNodeKindFromActionType(action.node_type);
  out.result_name = action.result_name;
  out.function_name = action.function_name;
  out.setExpression(action.result_name);
  if (out.kind == ExpressionNode::Kind::COLUMN || out.kind == ExpressionNode::Kind::INPUT) {
    auto source_name = action.column_name.empty() ? action.result_name : action.column_name;
    if (!action.column_name.empty() && action.column_name.starts_with("Const(")) {
      source_name = action.result_name;
    }
    out.source_name = stripClickHouseTypedLiterals(source_name);
  }
  for (const auto argument_id : action.arguments) {
    if (!action_program.nodes_by_internal_id.contains(argument_id)) {
      continue;
    }
    auto child = std::make_unique<ExpressionNode>();
    buildExpressionNodeFromActionTreeNoRender(*child, action_program, argument_id, plan_node_id);
    out.addChild(std::move(child));
  }
}

std::vector<ExpressionNode> parseActions(const std::string& node_id, const json& node_json) {
  if (!node_json.contains("Expression") || !node_json["Expression"].is_object()) {
    return {};
  }
  const auto& expr_json = node_json["Expression"];
  if (!expr_json.contains("Actions") || !expr_json["Actions"].is_array()) {
    return {};
  }
  const auto& raw_actions = expr_json["Actions"];
  const auto action_program = parseActionProgram(raw_actions);
  if (!action_program.has_value()) {
    return {};
  }
  std::vector<ExpressionNode> actions(action_program->topological_order.size());
  for (size_t i = 0; i < action_program->topological_order.size(); ++i) {
    const auto internal_id = action_program->topological_order[i];
    buildExpressionNodeFromActionTreeNoRender(actions[i], *action_program, internal_id, node_id);
  }
  return actions;
}

std::vector<ExpressionNode> parseOutputExpressions(const std::string& node_id, const json& node_json) {
  if (!node_json.contains("Expression") || !node_json["Expression"].is_object()) {
    return {};
  }
  const auto& expr_json = node_json["Expression"];
  if (!expr_json.contains("Actions") || !expr_json["Actions"].is_array()) {
    return {};
  }
  if (!expr_json.contains("Outputs") || !expr_json["Outputs"].is_array()) {
    return {};
  }
  if (!expr_json.contains("Positions") || !expr_json["Positions"].is_array()) {
    return {};
  }

  const auto action_program = parseActionProgram(expr_json["Actions"]);
  if (!action_program.has_value()) {
    return {};
  }

  const auto& outputs_json = expr_json["Outputs"];
  const auto& positions_json = expr_json["Positions"];
  const auto output_count = std::min(outputs_json.size(), positions_json.size());
  std::vector<ExpressionNode> outputs(output_count);
  for (size_t i = 0; i < output_count; ++i) {
    const auto output_name = outputs_json[i].contains("Name") && outputs_json[i]["Name"].is_string()
                               ? outputs_json[i]["Name"].get<std::string>()
                               : "";
    const auto output_type = outputs_json[i].contains("Type") && outputs_json[i]["Type"].is_string()
                               ? outputs_json[i]["Type"].get<std::string>()
                               : "";
    if (!positions_json[i].is_number_integer()) {
      outputs[i].kind = ExpressionNode::Kind::COLUMN;
      outputs[i].plan_node_id = node_id;
      outputs[i].output_name = output_name;
      outputs[i].output_type = output_type;
      outputs[i].result_name = output_name;
      outputs[i].source_name = output_name;
      outputs[i].setExpression(output_name);
      continue;
    }
    const auto result_slot = positions_json[i].get<int64_t>();
    if (!action_program->latest_internal_id_by_result_slot.contains(result_slot)) {
      outputs[i].kind = ExpressionNode::Kind::COLUMN;
      outputs[i].plan_node_id = node_id;
      outputs[i].output_name = output_name;
      outputs[i].output_type = output_type;
      outputs[i].result_name = output_name;
      outputs[i].source_name = output_name;
      outputs[i].setExpression(output_name);
      continue;
    }
    const auto internal_id = action_program->latest_internal_id_by_result_slot.at(result_slot);
    buildExpressionNodeFromActionTreeNoRender(outputs[i], *action_program, internal_id, node_id);
    outputs[i].output_name = output_name;
    outputs[i].output_type = output_type;
  }
  return outputs;
}

std::vector<ExpressionNode> parseFilterActionExpressions(const std::string& node_id,
                                                         const json& unresolved_actions,
                                                         const std::vector<std::string>& unresolved_filter_columns) {
  if (unresolved_filter_columns.empty()) {
    return {};
  }
  if (!unresolved_actions.is_array()) {
    throw std::runtime_error("Filter node '" + node_id + "' has filter columns but no Expression.Actions array");
  }

  const auto action_program = parseActionProgram(unresolved_actions);
  if (!action_program.has_value()) {
    throw std::runtime_error("Failed to parse Expression.Actions for filter node '" + node_id + "'");
  }

  std::vector<ExpressionNode> filter_columns(unresolved_filter_columns.size());
  for (size_t i = 0; i < unresolved_filter_columns.size(); ++i) {
    const auto& filter_column = unresolved_filter_columns[i];
    std::optional<int64_t> matched_internal_id;
    for (auto it = action_program->topological_order.rbegin();
         it != action_program->topological_order.rend();
         ++it) {
      const auto internal_id = *it;
      if (!action_program->nodes_by_internal_id.contains(internal_id)) {
        continue;
      }
      const auto& candidate = action_program->nodes_by_internal_id.at(internal_id);
      if (candidate.result_name == filter_column) {
        matched_internal_id = internal_id;
        break;
      }
    }
    if (!matched_internal_id.has_value()) {
      throw std::runtime_error("Failed to resolve filter column '" + filter_column + "' in node '" + node_id + "'");
    }
    buildExpressionNodeFromActionTreeNoRender(filter_columns[i], *action_program, *matched_internal_id, node_id);
    filter_columns[i].output_name = filter_column;
  }
  return filter_columns;
}

std::vector<ExpressionNode> parseExpressionActions(const std::string& node_id, const json& expression_json) {
  if (!expression_json.is_object() || !expression_json.contains("Actions") || !expression_json["Actions"].is_array()) {
    return {};
  }
  const auto action_program = parseActionProgram(expression_json["Actions"]);
  if (!action_program.has_value()) {
    return {};
  }
  std::vector<ExpressionNode> roots(action_program->topological_order.size());
  for (size_t i = 0; i < action_program->topological_order.size(); ++i) {
    const auto internal_id = action_program->topological_order[i];
    buildExpressionNodeFromActionTreeNoRender(roots[i], *action_program, internal_id, node_id);
  }
  return roots;
}

std::vector<ExpressionNode> buildNodeOwnedOutputExpressions(
    const std::string& node_id,
    const std::vector<PlanNodeHeader>& headers) {
  if (node_id.empty() || headers.empty()) {
    return {};
  }

  std::vector<ExpressionNode> outputs(headers.size());
  for (size_t i = 0; i < headers.size(); ++i) {
    const auto& header = headers[i];
    if (header.name.empty()) {
      continue;
    }
    auto& out = outputs[i];
    out.kind = ExpressionNode::Kind::COLUMN;
    out.leaf_binding = ExpressionNode::LeafBinding::NONE;
    out.plan_node_id = node_id;
    out.output_name = header.name;
    out.output_type = header.type;
    out.result_name = header.name;
    out.source_name = header.name;
    out.setExpression(header.name);
  }
  return outputs;
}

ExpressionNode* resolveToLocalOutput(const std::string& expression, std::vector<ExpressionNode>& outputs) {
  if (expression.empty()) {
    return nullptr;
  }
  size_t exact_matches = 0;
  ExpressionNode* exact_matched_root = nullptr;
  for (auto& output_root : outputs) {
    if (output_root.output_name.empty()) {
      continue;
    }
    if (expression == output_root.output_name) {
      ++exact_matches;
      exact_matched_root = &output_root;
    }
  }

  if (exact_matches == 1) {
    return exact_matched_root;
  }
  return nullptr;
}

ExpressionNode* resolveToLocalAggregate(const std::string& expression, std::vector<ExpressionNode>& aggregates) {
  if (expression.empty()) {
    return nullptr;
  }
  size_t matches = 0;
  ExpressionNode* matched = nullptr;
  for (auto& aggregate_root : aggregates) {
    const bool match_by_name = !aggregate_root.result_name.empty() && aggregate_root.result_name == expression;
    const auto aggregate_sql = aggregate_root.renderExecutableSql();
    const bool match_by_sql = !aggregate_sql.empty() && aggregate_sql == expression;
    if (!match_by_name && !match_by_sql) {
      continue;
    }
    ++matches;
    matched = &aggregate_root;
  }
  if (matches == 1) {
    return matched;
  }
  return nullptr;
}

ExpressionNode* resolveToLocalAggregateByRawJson(const std::string& expression, PlanNode& node) {
  if (!node.raw_json.contains("Aggregates") || !node.raw_json["Aggregates"].is_array()) {
    return nullptr;
  }
  if (expression.empty()) {
    return nullptr;
  }
  const auto& raw_aggregates = node.raw_json["Aggregates"];
  ExpressionNode* matched = nullptr;
  size_t matches = 0;
  for (size_t i = 0; i < raw_aggregates.size() && i < node.aggregates.size(); ++i) {
    if (!raw_aggregates[i].contains("Name") || !raw_aggregates[i]["Name"].is_string()) {
      continue;
    }
    const auto aggregate_name = raw_aggregates[i]["Name"].get<std::string>();
    if (aggregate_name.empty() || aggregate_name != expression) {
      continue;
    }
    ++matches;
    matched = &node.aggregates[i];
  }
  if (matches == 1) {
    return matched;
  }
  return nullptr;
}

std::unique_ptr<PlanNode> buildPlanNodeTreeRecursive(const json& node_json) {
  if (!node_json.is_object()) {
    throw std::runtime_error("Plan node must be a JSON object");
  }

  auto node = std::make_unique<PlanNode>();
  node->raw_json = node_json;
  node->node_type = node_json.value("Node Type", "");
  node->node_id = node_json.value("Node Id", "");
  node->description = node_json.value("Description", "");
  if (const auto prewhere_expression_json = parsePrewhereFilterExpressionJson(node_json); prewhere_expression_json.has_value()) {
    node->unresolved_prewhere_filter_expression = *prewhere_expression_json;
    node->prewhere_filter_expressions = parseExpressionActions(node->node_id, *prewhere_expression_json);
  } else {
    node->unresolved_prewhere_filter_expression = json::object();
  }
  node->headers = parseHeaders(node_json);
  node->unresolved_actions = node_json.contains("Expression") &&
                                 node_json["Expression"].contains("Actions")
                               ? node_json["Expression"]["Actions"]
                               : json::array();
  node->unresolved_input_name_by_slot = parseInputNamesBySlot(node_json);
  node->actions = parseActions(node->node_id, node_json);
  node->unresolved_filter_columns = parseFilterColumns(node_json);
  node->filter_columns = parseFilterActionExpressions(node->node_id, node->unresolved_actions, node->unresolved_filter_columns);
  node->unresolved_keys = parseKeys(node_json);
  {
    auto [sort_columns, sort_ascending] = parseSortColumns(node_json);
    node->unresolved_sort_ascending = std::move(sort_ascending);
    node->sort_columns = std::vector<ExpressionNode>(sort_columns.size());
    for (size_t i = 0; i < sort_columns.size(); ++i) {
      node->sort_columns[i].kind = ExpressionNode::Kind::COLUMN;
      node->sort_columns[i].plan_node_id = node->node_id;
      node->sort_columns[i].source_name = sort_columns[i];
      node->sort_columns[i].result_name = sort_columns[i];
      node->sort_columns[i].output_name = sort_columns[i];
    }
  }
  node->unresolved_clauses = node_json.value("Clauses", "");
  node->clauses = parseClausesToExpressions(node->node_id, node->unresolved_clauses);
  node->output_expressions = parseOutputExpressions(node->node_id, node_json);
  if (node->output_expressions.empty()) {
    node->output_expressions = buildNodeOwnedOutputExpressions(node->node_id, node->headers);
  }

  if (node_json.contains("Plans") && node_json["Plans"].is_array()) {
    for (const auto& child : node_json["Plans"]) {
      node->addChild(buildPlanNodeTreeRecursive(child));
    }
  }
  return node;
}

std::vector<ChildOutputCandidate> childOutputCandidates(PlanNode& node) {
  std::vector<ChildOutputCandidate> candidates;
  for (auto* child : node.children()) {
    if (child == nullptr || child->node_id.empty()) {
      continue;
    }
    // Parent nodes bind against child headers only. Each header is expected
    // to be wired to an output expression root on the child node.
    for (auto& header : child->headers) {
      if (header.name.empty()) {
        continue;
      }
      if (header.expression == nullptr) {
        ExpressionNode probe;
        probe.source_name = header.name;
        if (probe.isExists()) {
          continue;
        }
        throw std::runtime_error("Unwired child header '" + header.name + "' in node '" + child->node_id + "'");
      }
      candidates.push_back(ChildOutputCandidate{
        .child_node_id = child->node_id,
        .output_name = header.name,
        .root = header.expression.get(),
      });
    }
  }
  return candidates;
}

ExpressionNode* resolveToChildOutput(const std::string& expression,
                                     const std::vector<ChildOutputCandidate>& candidates) {
  if (expression.empty()) {
    return nullptr;
  }
  size_t exact_matches = 0;
  ExpressionNode* exact_matched_root = nullptr;
  for (const auto& candidate : candidates) {
    if (expression == candidate.output_name) {
      ++exact_matches;
      exact_matched_root = candidate.root;
    }
  }

  if (exact_matches == 1) {
    return exact_matched_root;
  }
  return nullptr;
}

ExpressionNode* resolveInputByInputSlot(const PlanNode& node,
                                        const ExpressionNode& expression,
                                        const std::vector<ChildOutputCandidate>& candidates) {
  if (expression.kind != ExpressionNode::Kind::INPUT || expression.input_slot < 0) {
    return nullptr;
  }
  if (!node.unresolved_input_name_by_slot.contains(expression.input_slot)) {
    PLOGD << "Input slot missing in Expression.Inputs node_id=" << node.node_id
          << " input_slot=" << expression.input_slot
          << " result_slot=" << expression.result_slot;
    return nullptr;
  }
  const auto& expected_input_name = node.unresolved_input_name_by_slot.at(expression.input_slot);
  auto* resolved = resolveToChildOutput(expected_input_name, candidates);
  if (resolved != nullptr) {
    return resolved;
  }

  std::string child_outputs;
  for (size_t i = 0; i < candidates.size(); ++i) {
    if (i > 0) {
      child_outputs += ", ";
    }
    child_outputs += candidates[i].output_name;
  }
  PLOGD << "Failed exact INPUT slot wiring node_id=" << node.node_id
        << " input_slot=" << expression.input_slot
        << " result_slot=" << expression.result_slot
        << " expected_input='" << expected_input_name
        << "' expression_leaf='" << expression.source_name
        << "' child_outputs=[" << child_outputs << "]";
  return nullptr;
}

void assignResolvedLeaf(ExpressionNode& leaf,
                        const std::string& expression,
                        ExpressionNode* child_root,
                        const std::string& owner_plan_node_id = "") {
  if (child_root == nullptr) {
    throw std::runtime_error("Cannot resolve expression leaf without child root");
  }
  leaf.kind = ExpressionNode::Kind::REFERENCE;
  if (!owner_plan_node_id.empty()) {
    leaf.plan_node_id = owner_plan_node_id;
  }
  if (leaf.output_name.empty()) {
    leaf.output_name = expression;
  }
  leaf.result_name = expression;
  leaf.source_name = expression;
  leaf.references = std::shared_ptr<ExpressionNode>(child_root, [](ExpressionNode*) {});
  leaf.leaf_binding = ExpressionNode::LeafBinding::CHILD_OUTPUT;

  leaf.linked_child_node_id = child_root->plan_node_id;
  leaf.linked_child_output_name = child_root->output_name;
  leaf.linked_child_output_root = child_root;
}

void wireHeaders(PlanNode& node, const std::vector<ChildOutputCandidate>& candidates) {
  for (size_t i = 0; i < node.headers.size(); ++i) {
    auto& header = node.headers[i];
    ExpressionNode* resolved_root = resolveToLocalOutput(header.name, node.output_expressions);
    if (resolved_root == nullptr) {
      resolved_root = resolveToLocalAggregateByRawJson(header.name, node);
    }
    if (resolved_root == nullptr) {
      resolved_root = resolveToLocalAggregate(header.name, node.aggregates);
    }
    if (resolved_root == nullptr) {
      resolved_root = resolveToChildOutput(header.name, candidates);
    }
    if (resolved_root == nullptr) {
      continue;
    }
    header.expression = std::make_unique<ExpressionNode>();
    assignResolvedLeaf(*header.expression, header.name, resolved_root, node.node_id);
  }
}

void wireKeys(PlanNode& node, const std::vector<ChildOutputCandidate>& candidates) {
  node.keys = std::vector<ExpressionNode>(node.unresolved_keys.size());
  for (size_t i = 0; i < node.unresolved_keys.size(); ++i) {
    const auto& key = node.unresolved_keys[i];
    auto* child_root = resolveToChildOutput(key, candidates);
    if (child_root == nullptr) {
      throw std::runtime_error("Failed to resolve key '" + key + "' in plan node '" + node.node_id + "'");
    }
    assignResolvedLeaf(node.keys[i], key, child_root, node.node_id);
  }
}

void wireSortColumns(PlanNode& node, const std::vector<ChildOutputCandidate>& candidates) {
  for (auto& sort_expression : node.sort_columns) {
    const auto lookup_name = !sort_expression.source_name.empty() ? sort_expression.source_name : sort_expression.result_name;
    if (lookup_name.empty()) {
      continue;
    }
    auto* child_root = resolveToChildOutput(lookup_name, candidates);
    if (child_root == nullptr) {
      continue;
    }
    assignResolvedLeaf(sort_expression, lookup_name, child_root, node.node_id);
  }
}

void wireAggregates(PlanNode& node, const std::vector<ChildOutputCandidate>& candidates) {
  if (!node.raw_json.contains("Aggregates") || !node.raw_json["Aggregates"].is_array()) {
    node.aggregates.clear();
    return;
  }
  const auto& raw_aggregates = node.raw_json["Aggregates"];
  node.aggregates = std::vector<ExpressionNode>(raw_aggregates.size());
  for (size_t i = 0; i < raw_aggregates.size(); ++i) {
    const auto& aggregate_json = raw_aggregates[i];
    if (!aggregate_json.contains("Function") || !aggregate_json["Function"].is_object()) {
      throw std::runtime_error("Aggregate in node '" + node.node_id + "' missing Function object");
    }
    const auto fn_name = aggregate_json["Function"].value("Name", "");
    if (fn_name.empty()) {
      throw std::runtime_error("Aggregate in node '" + node.node_id + "' missing Function.Name");
    }

    auto& aggregate_root = node.aggregates[i];
    aggregate_root.kind = ExpressionNode::Kind::FUNCTION;
    aggregate_root.function_name = fn_name;
    aggregate_root.result_name = aggregate_json.value("Name", "");

    if (!aggregate_json.contains("Arguments") || !aggregate_json["Arguments"].is_array()) {
      continue;
    }
    for (const auto& arg_json : aggregate_json["Arguments"]) {
      if (!arg_json.is_string()) {
        continue;
      }
      const auto argument = arg_json.get<std::string>();
      auto* child_root = resolveToChildOutput(argument, candidates);
      if (child_root == nullptr) {
        throw std::runtime_error("Failed to resolve aggregate argument '" + argument + "' in plan node '" + node.node_id + "'");
      }
      auto arg_leaf = std::make_unique<ExpressionNode>();
      assignResolvedLeaf(*arg_leaf, argument, child_root, node.node_id);
      aggregate_root.addChild(std::move(arg_leaf));
    }
  }
}

void wireClauses(PlanNode& node, const std::vector<ChildOutputCandidate>& candidates) {
  for (auto& clause_root : node.clauses) {
    for (auto& expr_node : clause_root.depth_first()) {
      if (expr_node.childCount() > 0) {
        continue;
      }
      const auto lookup_name = !expr_node.source_name.empty() ? expr_node.source_name : expr_node.result_name;
      if (lookup_name.empty()) {
        continue;
      }
      auto* child_root = resolveToChildOutput(lookup_name, candidates);
      if (child_root == nullptr) {
        throw std::runtime_error("Failed to resolve clause expression leaf '" + lookup_name + "' in plan node '" + node.node_id + "'");
      }
      assignResolvedLeaf(expr_node, lookup_name, child_root, node.node_id);
    }
  }
}

void wireActions(PlanNode& node, const std::vector<ChildOutputCandidate>& candidates) {
  for (auto& action_root : node.actions) {
    for (auto& expr_node : action_root.depth_first()) {
      if (expr_node.childCount() > 0) {
        continue;
      }
      if (expr_node.kind != ExpressionNode::Kind::COLUMN && expr_node.kind != ExpressionNode::Kind::INPUT) {
        continue;
      }
      if (expr_node.leaf_binding == ExpressionNode::LeafBinding::CHILD_OUTPUT) {
        continue;
      }
      const auto lookup_name = !expr_node.source_name.empty() ? expr_node.source_name : expr_node.result_name;
      if (lookup_name.empty()) {
        continue;
      }
      if (isSimpleConstantLiteral(lookup_name)) {
        continue;
      }
      if (auto* local_aggregate = resolveToLocalAggregateByRawJson(lookup_name, node); local_aggregate != nullptr) {
        assignResolvedLeaf(expr_node, lookup_name, local_aggregate, node.node_id);
        continue;
      }
      if (auto* local_aggregate = resolveToLocalAggregate(lookup_name, node.aggregates); local_aggregate != nullptr) {
        assignResolvedLeaf(expr_node, lookup_name, local_aggregate, node.node_id);
        continue;
      }
      ExpressionNode* child_root = nullptr;
      if (expr_node.kind == ExpressionNode::Kind::INPUT) {
        child_root = resolveInputByInputSlot(node, expr_node, candidates);
      } else {
        child_root = resolveToChildOutput(lookup_name, candidates);
      }
      if (child_root == nullptr) {
        PLOGD << "Failed leaf wiring in actions node_id=" << node.node_id
              << " kind=" << static_cast<int>(expr_node.kind)
              << " lookup='" << lookup_name << "'";
        continue;
      }
      assignResolvedLeaf(expr_node, lookup_name, child_root, node.node_id);
    }
  }
}

void wireFilterColumns(PlanNode& node, const std::vector<ChildOutputCandidate>& candidates) {
  for (auto& filter_root : node.filter_columns) {
    for (auto& expr_node : filter_root.depth_first()) {
      if (expr_node.childCount() > 0) {
        continue;
      }
      if (expr_node.kind != ExpressionNode::Kind::COLUMN && expr_node.kind != ExpressionNode::Kind::INPUT) {
        continue;
      }
      if (expr_node.leaf_binding == ExpressionNode::LeafBinding::CHILD_OUTPUT) {
        continue;
      }
      const auto lookup_name = !expr_node.source_name.empty() ? expr_node.source_name : expr_node.result_name;
      if (lookup_name.empty()) {
        continue;
      }
      if (isSimpleConstantLiteral(lookup_name)) {
        continue;
      }
      if (auto* local_aggregate = resolveToLocalAggregateByRawJson(lookup_name, node); local_aggregate != nullptr) {
        assignResolvedLeaf(expr_node, lookup_name, local_aggregate, node.node_id);
        continue;
      }
      if (auto* local_aggregate = resolveToLocalAggregate(lookup_name, node.aggregates); local_aggregate != nullptr) {
        assignResolvedLeaf(expr_node, lookup_name, local_aggregate, node.node_id);
        continue;
      }
      ExpressionNode* child_root = nullptr;
      if (expr_node.kind == ExpressionNode::Kind::INPUT) {
        child_root = resolveInputByInputSlot(node, expr_node, candidates);
      } else {
        child_root = resolveToChildOutput(lookup_name, candidates);
      }
      if (child_root == nullptr) {
        PLOGD << "Failed leaf wiring in filter_columns node_id=" << node.node_id
              << " kind=" << static_cast<int>(expr_node.kind)
              << " lookup='" << lookup_name << "'";
        continue;
      }
      assignResolvedLeaf(expr_node, lookup_name, child_root, node.node_id);
    }
  }
}

void wirePrewhereFilterExpressions(PlanNode& node, const std::vector<ChildOutputCandidate>& candidates) {
  for (auto& prewhere_root : node.prewhere_filter_expressions) {
    for (auto& expr_node : prewhere_root.depth_first()) {
      if (expr_node.childCount() > 0) {
        continue;
      }
      if (expr_node.kind != ExpressionNode::Kind::COLUMN && expr_node.kind != ExpressionNode::Kind::INPUT) {
        continue;
      }
      if (expr_node.leaf_binding == ExpressionNode::LeafBinding::CHILD_OUTPUT) {
        continue;
      }
      const auto lookup_name = !expr_node.source_name.empty() ? expr_node.source_name : expr_node.result_name;
      if (lookup_name.empty()) {
        continue;
      }
      if (isSimpleConstantLiteral(lookup_name)) {
        continue;
      }
      if (auto* local_aggregate = resolveToLocalAggregateByRawJson(lookup_name, node); local_aggregate != nullptr) {
        assignResolvedLeaf(expr_node, lookup_name, local_aggregate, node.node_id);
        continue;
      }
      if (auto* local_aggregate = resolveToLocalAggregate(lookup_name, node.aggregates); local_aggregate != nullptr) {
        assignResolvedLeaf(expr_node, lookup_name, local_aggregate, node.node_id);
        continue;
      }
      ExpressionNode* child_root = nullptr;
      if (expr_node.kind == ExpressionNode::Kind::INPUT) {
        child_root = resolveInputByInputSlot(node, expr_node, candidates);
      } else {
        child_root = resolveToChildOutput(lookup_name, candidates);
      }
      if (child_root == nullptr) {
        PLOGD << "Failed leaf wiring in prewhere node_id=" << node.node_id
              << " kind=" << static_cast<int>(expr_node.kind)
              << " lookup='" << lookup_name << "'";
        continue;
      }
      assignResolvedLeaf(expr_node, lookup_name, child_root, node.node_id);
    }
  }
}

void wireOutputs(PlanNode& node, const std::vector<ChildOutputCandidate>& candidates) {
  for (auto& output_root : node.output_expressions) {
    for (auto& expr_node : output_root.depth_first()) {
      if (expr_node.childCount() > 0) {
        continue;
      }
      if (expr_node.kind != ExpressionNode::Kind::COLUMN && expr_node.kind != ExpressionNode::Kind::INPUT) {
        continue;
      }
      if (expr_node.leaf_binding == ExpressionNode::LeafBinding::CHILD_OUTPUT) {
        continue;
      }
      const auto lookup_name = !expr_node.source_name.empty() ? expr_node.source_name : expr_node.result_name;
      if (lookup_name.empty()) {
        continue;
      }
      if (isSimpleConstantLiteral(lookup_name)) {
        continue;
      }
      if (auto* local_aggregate = resolveToLocalAggregateByRawJson(lookup_name, node); local_aggregate != nullptr) {
        assignResolvedLeaf(expr_node, lookup_name, local_aggregate, node.node_id);
        continue;
      }
      if (auto* local_aggregate = resolveToLocalAggregate(lookup_name, node.aggregates); local_aggregate != nullptr) {
        assignResolvedLeaf(expr_node, lookup_name, local_aggregate, node.node_id);
        continue;
      }
      ExpressionNode* child_root = nullptr;
      if (expr_node.kind == ExpressionNode::Kind::INPUT) {
        child_root = resolveInputByInputSlot(node, expr_node, candidates);
      } else {
        child_root = resolveToChildOutput(lookup_name, candidates);
      }
      if (child_root == nullptr) {
        PLOGD << "Failed leaf wiring in outputs node_id=" << node.node_id
              << " kind=" << static_cast<int>(expr_node.kind)
              << " lookup='" << lookup_name << "'";
        continue;
      }
      assignResolvedLeaf(expr_node, lookup_name, child_root, node.node_id);
    }
  }
}

void wirePlanNode(PlanNode& node) {
  const auto candidates = childOutputCandidates(node);

  wireHeaders(node, candidates);
  wireKeys(node, candidates);
  wireSortColumns(node, candidates);
  wireAggregates(node, candidates);
  wireClauses(node, candidates);
  wireActions(node, candidates);
  wireFilterColumns(node, candidates);
  wirePrewhereFilterExpressions(node, candidates);
  wireOutputs(node, candidates);

  std::string diagnostics;
  if (!node.validate(&diagnostics)) {
    PLOGE << diagnostics;
    throw std::runtime_error(diagnostics);
  }
}

int parseNodeIdSuffix(const std::string& node_id) {
  const auto pos = node_id.find_last_of('_');
  if (pos == std::string::npos || pos + 1 >= node_id.size()) {
    return -1;
  }
  const auto suffix = node_id.substr(pos + 1);
  if (suffix.empty()) {
    return -1;
  }
  for (const auto c : suffix) {
    if (!std::isdigit(static_cast<unsigned char>(c))) {
      return -1;
    }
  }
  try {
    return std::stoi(suffix);
  } catch (...) {
    return -1;
  }
}

void indexPlanNodesById(PlanNode& root, std::map<std::string, PlanNode*>& by_id) {
  by_id.clear();
  for (auto& node : root.depth_first()) {
    if (node.node_id.empty()) {
      continue;
    }
    by_id[node.node_id] = &node;
  }
}

void wireCommonBuffers(PlanNode& root) {
  std::map<std::string, PlanNode*> by_id;
  indexPlanNodesById(root, by_id);
  for (auto& node : root.depth_first()) {
    if (node.node_type != "ReadFromCommonBuffer") {
      continue;
    }
    const auto parsed_node_id = parseNodeIdSuffix(node.node_id);
    if (parsed_node_id < 0) {
      continue;
    }
    const auto producer_node_id = "SaveSubqueryResultToBuffer_" + std::to_string(parsed_node_id + 1);
    if (!by_id.contains(producer_node_id)) {
      continue;
    }
    auto* producer = by_id.at(producer_node_id);
    if (producer == nullptr || producer->node_type != "SaveSubqueryResultToBuffer") {
      continue;
    }
    node.common_buffer_producer = producer;
  }
}

void rewireCommonBufferLineage(PlanNode& root) {
  for (auto& node : root.depth_first()) {
    if (node.node_type != "ReadFromCommonBuffer" || node.common_buffer_producer == nullptr) {
      continue;
    }
    auto* producer = node.common_buffer_producer;
    if (producer == nullptr || producer == &node) {
      continue;
    }

    std::map<std::string, ExpressionNode*> producer_header_expr_by_name;
    for (auto& producer_header : producer->headers) {
      if (producer_header.name.empty() || producer_header.expression == nullptr) {
        continue;
      }
      producer_header_expr_by_name[producer_header.name] = producer_header.expression.get();
    }

    for (auto& output_root : node.output_expressions) {
      if (output_root.output_name.empty()) {
        continue;
      }
      if (!producer_header_expr_by_name.contains(output_root.output_name)) {
        continue;
      }
      auto* producer_expr = producer_header_expr_by_name.at(output_root.output_name);
      if (producer_expr == nullptr) {
        continue;
      }
      assignResolvedLeaf(output_root, output_root.output_name, producer_expr, node.node_id);
    }

    for (auto& header : node.headers) {
      if (header.name.empty()) {
        continue;
      }
      if (!producer_header_expr_by_name.contains(header.name)) {
        continue;
      }
      auto* producer_expr = producer_header_expr_by_name.at(header.name);
      if (producer_expr == nullptr) {
        continue;
      }
      if (header.expression == nullptr) {
        header.expression = std::make_unique<ExpressionNode>();
      }
      assignResolvedLeaf(*header.expression, header.name, producer_expr, node.node_id);
    }
  }
}

std::string unqualifyColumnName(std::string value) {
  value = cleanExpression(trim_string(std::move(value)));
  const auto dot = value.find_last_of('.');
  if (dot == std::string::npos || dot + 1 >= value.size()) {
    return value;
  }
  return value.substr(dot + 1);
}

bool textLooksExistsDerived(const std::string& value) {
  if (value.empty()) {
    return false;
  }
  ExpressionNode probe;
  probe.source_name = value;
  return probe.isExists();
}

std::string resolveExistsLeafBaseNameFromLineage(const ExpressionNode& leaf) {
  std::set<const ExpressionNode*> visited;
  std::vector<const ExpressionNode*> stack;
  stack.push_back(&leaf);
  while (!stack.empty()) {
    const auto* current = stack.back();
    stack.pop_back();
    if (current == nullptr || visited.contains(current)) {
      continue;
    }
    visited.insert(current);

    if (!current->linked_child_output_name.empty() &&
        !textLooksExistsDerived(current->linked_child_output_name)) {
      return unqualifyColumnName(current->linked_child_output_name);
    }
    if (!current->output_name.empty() && !textLooksExistsDerived(current->output_name)) {
      return unqualifyColumnName(current->output_name);
    }
    if (!current->source_name.empty() && !textLooksExistsDerived(current->source_name)) {
      return unqualifyColumnName(current->source_name);
    }
    if (!current->result_name.empty() && !textLooksExistsDerived(current->result_name)) {
      return unqualifyColumnName(current->result_name);
    }

    if (current->references != nullptr) {
      stack.push_back(current->references.get());
    }
    if (current->linked_child_output_root != nullptr) {
      stack.push_back(current->linked_child_output_root);
    }
    for (const auto* child : current->children()) {
      if (child != nullptr) {
        stack.push_back(child);
      }
    }
  }
  return "";
}

std::string resolveExistsLeafAliasFromLineage(const ExpressionNode& leaf) {
  std::set<const ExpressionNode*> visited;
  std::vector<const ExpressionNode*> stack;
  stack.push_back(&leaf);
  while (!stack.empty()) {
    const auto* current = stack.back();
    stack.pop_back();
    if (current == nullptr || visited.contains(current)) {
      continue;
    }
    visited.insert(current);

    if (!current->alias_user.empty() && current->kind != ExpressionNode::Kind::FUNCTION) {
      return sanitizeIdentifier(current->alias_user);
    }

    if (current->references != nullptr) {
      stack.push_back(current->references.get());
    }
    if (current->linked_child_output_root != nullptr) {
      stack.push_back(current->linked_child_output_root);
    }
    for (const auto* child : current->children()) {
      if (child != nullptr) {
        stack.push_back(child);
      }
    }
  }
  return "";
}

void normalizeExistsJoinClausePredicatesFromLineage(PlanNode& root) {
  for (auto& node : root.depth_first()) {
    if (to_upper(node.node_type) != "JOIN") {
      continue;
    }
    if (node.childCount() < 2) {
      continue;
    }
    const auto first_child_id = node.firstChild() != nullptr ? node.firstChild()->node_id : "";
    const auto last_child_id = node.lastChild() != nullptr ? node.lastChild()->node_id : "";

    for (auto& clause_root : node.clauses) {
      for (auto& expr : clause_root.depth_first()) {
        if (expr.childCount() > 0 || expr.kind != ExpressionNode::Kind::REFERENCE) {
          continue;
        }
        const bool exists_derived =
            textLooksExistsDerived(expr.linked_child_output_name) ||
            textLooksExistsDerived(expr.output_name) ||
            textLooksExistsDerived(expr.source_name) ||
            textLooksExistsDerived(expr.result_name) ||
            expressionTreeContainsExistsRoot(expr);
        if (!exists_derived) {
          continue;
        }

        auto normalized_name = resolveExistsLeafAliasFromLineage(expr);
        if (normalized_name.empty()) {
          auto base = resolveExistsLeafBaseNameFromLineage(expr);
          if (base.empty()) {
            continue;
          }
          std::string suffix;
          if (!first_child_id.empty() && expr.linked_child_node_id == first_child_id) {
            suffix = "_right";
          } else if (!last_child_id.empty() && expr.linked_child_node_id == last_child_id) {
            suffix = "_left";
          } else {
            suffix = "_right";
          }
          normalized_name = base + suffix;
        }
        expr.output_name = normalized_name;
        expr.result_name = normalized_name;
        expr.source_name = normalized_name;
        expr.setExpression(normalized_name);
        expr.linked_child_output_name = normalized_name;
      }
    }
  }
}

bool isLeafLevelExpressionOrReferenceToLeaf(const ExpressionNode& node) {
  if (node.kind == ExpressionNode::Kind::REFERENCE &&
      node.childCount() == 1 &&
      node.firstChild() != nullptr) {
    return isLeafLevelExpressionOrReferenceToLeaf(*node.firstChild());
  }
  return node.childCount() == 0;
}

bool isSyntheticAliasName(const std::string& value) {
  const auto trimmed = trim_string(value);
  if (trimmed.empty()) {
    return false;
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

bool isNonSyntheticAliasNode(const ExpressionNode& node) {
  if (node.kind != ExpressionNode::Kind::ALIAS) {
    return false;
  }
  return !isSyntheticAliasName(node.output_name) &&
         !isSyntheticAliasName(node.result_name) &&
         !isSyntheticAliasName(node.source_name);
}

std::string normalizeAliasCandidate(std::string candidate) {
  candidate = cleanExpression(trim_string(std::move(candidate)));
  if (candidate.empty()) {
    return "";
  }
  if (isSyntheticAliasName(candidate)) {
    return "";
  }
  // Only accept plain SQL identifiers as propagated semantic aliases.
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

std::string findSemanticAliasInLineage(const ExpressionNode& root) {
  std::set<const ExpressionNode*> visited;
  const ExpressionNode* current = &root;
  while (current != nullptr) {
    if (visited.contains(current)) {
      break;
    }
    visited.insert(current);

    if (current->kind == ExpressionNode::Kind::ALIAS) {
      for (const auto* raw : {&current->output_name, &current->result_name, &current->source_name}) {
        const auto candidate = normalizeAliasCandidate(*raw);
        if (!candidate.empty()) {
          return candidate;
        }
      }
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
      break;
    }

    if (current->kind == ExpressionNode::Kind::FUNCTION) {
      // Do not propagate aliases through semantic transformations (e.g. SUM(volume)).
      break;
    }

    if (current->childCount() == 1 && current->firstChild() != nullptr) {
      current = current->firstChild();
      continue;
    }
    break;
  }
  return "";
}

std::string nearestNonSyntheticAliasName(const ExpressionNode& node) {
  const ExpressionNode* current = &node;
  while (current != nullptr && !current->isRoot()) {
    current = &current->parent();
    if (!isNonSyntheticAliasNode(*current)) {
      continue;
    }
    if (!current->output_name.empty()) {
      return current->output_name;
    }
    if (!current->result_name.empty()) {
      return current->result_name;
    }
    if (!current->source_name.empty()) {
      return current->source_name;
    }
  }
  return "";
}

std::string sanitizeIdentifier(std::string token) {
  if (token.empty()) {
    return "";
  }
  for (auto& c : token) {
    if (!(std::isalnum(static_cast<unsigned char>(c)) != 0 || c == '_')) {
      c = '_';
    }
  }
  if (!(std::isalpha(static_cast<unsigned char>(token.front())) != 0 || token.front() == '_')) {
    token = "c_" + token;
  }
  return token;
}

std::string topLevelName(std::string value) {
  value = cleanExpression(trim_string(std::move(value)));
  const auto dot = value.find_last_of('.');
  if (dot == std::string::npos || dot + 1 >= value.size()) {
    return value;
  }
  return value.substr(dot + 1);
}

ExpressionNode* aliasTargetFromHeaderExpression(ExpressionNode* header_expression) {
  if (header_expression == nullptr) {
    return nullptr;
  }
  if (header_expression->references != nullptr) {
    return header_expression->references.get();
  }
  return header_expression;
}

void assignJoinInputDisambiguationAliases(PlanNode& root) {
  struct SideAliasRef {
    ExpressionNode* expression = nullptr;
    bool from_left = true;
    std::string base_name;
  };

  for (auto& node : root.depth_first()) {
    if (to_upper(node.node_type) != "JOIN" || node.childCount() < 2) {
      continue;
    }

    const auto* left = node.firstChild();
    const auto* right = node.lastChild();
    if (left == nullptr || right == nullptr) {
      continue;
    }

    std::map<std::string, std::vector<SideAliasRef>> refs_by_name_upper;
    const auto candidates = childOutputCandidates(node);
    for (const auto& candidate : candidates) {
      if (candidate.root == nullptr || candidate.output_name.empty()) {
        continue;
      }
      const auto base_name = topLevelName(candidate.output_name);
      if (base_name.empty()) {
        continue;
      }
      const bool is_left = candidate.child_node_id == left->node_id;
      const bool is_right = candidate.child_node_id == right->node_id;
      if (!is_left && !is_right) {
        continue;
      }
      auto* target = aliasTargetFromHeaderExpression(candidate.root);
      if (target == nullptr) {
        continue;
      }
      refs_by_name_upper[to_upper(base_name)].push_back(SideAliasRef{
        .expression = target,
        .from_left = is_left,
        .base_name = base_name,
      });
    }

    for (auto& [name_upper, refs] : refs_by_name_upper) {
      (void)name_upper;
      bool has_left = false;
      bool has_right = false;
      for (const auto& ref : refs) {
        if (ref.from_left) {
          has_left = true;
        } else {
          has_right = true;
        }
      }
      if (!has_left || !has_right) {
        continue;
      }

      size_t left_index = 0;
      size_t right_index = 0;
      for (auto& ref : refs) {
        if (ref.expression == nullptr || !ref.expression->alias_user.empty()) {
          continue;
        }
        auto alias = sanitizeIdentifier(ref.base_name + (ref.from_left ? "_left" : "_right"));
        if (ref.from_left && left_index++ > 0) {
          alias += "_" + std::to_string(left_index);
        }
        if (!ref.from_left && right_index++ > 0) {
          alias += "_" + std::to_string(right_index);
        }
        ref.expression->alias_user = alias;
      }
    }
  }
}

void assignAliasesForExpressionRoot(ExpressionNode& root, uint64_t& next_sql, uint64_t& next_user) {
  for (auto& expr : root.depth_first()) {
    expr.alias_sql = "s" + std::to_string(next_sql++);
    if (expr.kind == ExpressionNode::Kind::FUNCTION) {
      const auto existing_alias = nearestNonSyntheticAliasName(expr);
      if (!existing_alias.empty()) {
        expr.alias_user = existing_alias;
      } else {
        expr.alias_user = "a" + std::to_string(next_user++);
      }
    } else {
      expr.alias_user.clear();
    }
  }
}

void assignExpressionAliases(PlanNode& root) {
  uint64_t next_sql = 1;
  uint64_t next_user = 1;
  for (auto& node : root.depth_first()) {
    for (auto& expr : node.prewhere_filter_expressions) {
      assignAliasesForExpressionRoot(expr, next_sql, next_user);
    }
    for (auto& expr : node.actions) {
      assignAliasesForExpressionRoot(expr, next_sql, next_user);
    }
    for (auto& expr : node.keys) {
      assignAliasesForExpressionRoot(expr, next_sql, next_user);
    }
    for (auto& expr : node.sort_columns) {
      assignAliasesForExpressionRoot(expr, next_sql, next_user);
    }
    for (auto& expr : node.aggregates) {
      assignAliasesForExpressionRoot(expr, next_sql, next_user);
    }
    for (auto& expr : node.filter_columns) {
      assignAliasesForExpressionRoot(expr, next_sql, next_user);
    }
    for (auto& expr : node.clauses) {
      assignAliasesForExpressionRoot(expr, next_sql, next_user);
    }
    for (auto& expr : node.output_expressions) {
      assignAliasesForExpressionRoot(expr, next_sql, next_user);
      if (expr.alias_user.empty()) {
        expr.alias_user = findSemanticAliasInLineage(expr);
      }
    }
    for (auto& header : node.headers) {
      if (header.expression == nullptr) {
        continue;
      }
      assignAliasesForExpressionRoot(*header.expression, next_sql, next_user);
    }
  }
  assignJoinInputDisambiguationAliases(root);
}

bool expressionTreeContainsExistsRoot(const ExpressionNode& root) {
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

bool headerContainsExists(const PlanNodeHeader& header) {
  if (header.expression != nullptr && expressionTreeContainsExistsRoot(*header.expression)) {
    return true;
  }
  if (header.name.empty()) {
    return false;
  }
  ExpressionNode probe;
  probe.source_name = header.name;
  return probe.isExists();
}

template <typename Container>
void pruneExistsRoots(Container& roots) {
  roots.erase(
      std::remove_if(
          roots.begin(),
          roots.end(),
          [](const auto& root) { return expressionTreeContainsExistsRoot(root); }),
      roots.end());
}

void pruneExistsExpressionTrees(PlanNode& root) {
  for (auto& node : root.depth_first()) {
    pruneExistsRoots(node.prewhere_filter_expressions);
    pruneExistsRoots(node.actions);
    pruneExistsRoots(node.keys);
    pruneExistsRoots(node.sort_columns);
    pruneExistsRoots(node.aggregates);
    pruneExistsRoots(node.filter_columns);
    // Keep clauses/outputs/headers intact after wiring; parents may still
    // reference these roots via lineage pointers.
  }
}

std::string expressionNodeSummary(const ExpressionNode& node) {
  std::string label;
  switch (node.kind) {
    case ExpressionNode::Kind::REFERENCE:
      label = "REFERENCE";
      break;
    case ExpressionNode::Kind::FUNCTION:
      label = "FUNCTION";
      break;
    case ExpressionNode::Kind::ALIAS:
      label = "ALIAS";
      break;
    case ExpressionNode::Kind::COLUMN:
      label = "COLUMN";
      break;
    case ExpressionNode::Kind::INPUT:
      label = "INPUT";
      break;
    case ExpressionNode::Kind::UNKNOWN:
    default:
      label = "UNKNOWN";
      break;
  }
  const auto name = !node.output_name.empty() ? node.output_name
                    : !node.result_name.empty() ? node.result_name
                    : !node.source_name.empty() ? node.source_name
                    : "";
  std::ostringstream out;
  out << label;
  if (!name.empty()) {
    out << " '" << name << "'";
  }
  if (!node.plan_node_id.empty()) {
    out << " @" << node.plan_node_id;
  }
  return out.str();
}

std::string unwrapTopLevelCastLiteralText(std::string value) {
  value = trim_string(std::move(value));
  const auto upper = to_upper(value);
  size_t prefix_len = 0;
  if (upper.starts_with("_CAST(")) {
    prefix_len = 6;
  } else if (upper.starts_with("CAST(")) {
    prefix_len = 5;
  } else {
    return value;
  }
  if (value.size() <= prefix_len || value.back() != ')') {
    return value;
  }

  const auto inside = value.substr(prefix_len, value.size() - prefix_len - 1);
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
      continue;
    }
    if (c == ')') {
      if (depth > 0) {
        --depth;
      }
      continue;
    }
    if (c == ',' && depth == 0) {
      return trim_string(inside.substr(0, i));
    }
  }
  return value;
}

bool isQuotedLiteralWithOptionalTypeSuffix(const std::string& value) {
  if (value.size() < 2 || value.front() != '\'') {
    return false;
  }
  bool in_single_quote = true;
  for (size_t i = 1; i < value.size(); ++i) {
    if (value[i] != '\'') {
      continue;
    }
    if (i + 1 < value.size() && value[i + 1] == '\'') {
      ++i;
      continue;
    }
    in_single_quote = false;
    const auto suffix = trim_string(value.substr(i + 1));
    return suffix.empty() ||
           suffix.starts_with("_") ||
           std::all_of(suffix.begin(), suffix.end(), [](const char c) {
             return c == ')';
           });
  }
  return !in_single_quote;
}

bool isSimpleConstantLiteral(const std::string& value) {
  const auto trimmed = trim_string(value);
  if (trimmed.empty()) {
    return false;
  }
  const auto unwrapped = unwrapTopLevelCastLiteralText(trimmed);
  const auto upper = to_upper(unwrapped);
  if (upper == "NULL" || upper == "TRUE" || upper == "FALSE") {
    return true;
  }
  if (isQuotedLiteralWithOptionalTypeSuffix(unwrapped)) {
    return true;
  }
  const auto stripped = stripClickHouseTypedLiterals(unwrapped);
  static const std::regex numeric(R"(^[+-]?([0-9]+(\.[0-9]*)?|\.[0-9]+)$)");
  return std::regex_match(stripped, numeric);
}

bool isAcceptableLeaf(const ExpressionNode& leaf,
                      const std::map<std::string, PlanNode*>& by_id,
                      std::string& reason) {
  if (leaf.kind == ExpressionNode::Kind::FUNCTION &&
      to_upper(leaf.function_name) == "COUNT" &&
      leaf.childCount() == 0) {
    return true;
  }

  const auto value = !leaf.source_name.empty() ? leaf.source_name : leaf.result_name;
  if (isSimpleConstantLiteral(value)) {
    return true;
  }

  if (leaf.leaf_binding == ExpressionNode::LeafBinding::BASE_TABLE) {
    return true;
  }

  const auto leaf_name = !leaf.source_name.empty() ? leaf.source_name : leaf.result_name;
  if (to_lower(leaf_name).starts_with("__set_")) {
    return true;
  }
  if (to_lower(leaf_name).starts_with("exists(")) {
    return true;
  }

  if (leaf.plan_node_id.empty()) {
    reason = "leaf has empty plan_node_id and is not recognized as constant";
    return false;
  }
  if (!by_id.contains(leaf.plan_node_id)) {
    reason = "leaf plan_node_id not found in subtree: " + leaf.plan_node_id;
    return false;
  }
  const auto* source_node = by_id.at(leaf.plan_node_id);
  if (source_node == nullptr) {
    reason = "leaf plan_node_id resolved to null node";
    return false;
  }
  if (source_node->node_type == "ReadFromMergeTree" ||
      source_node->node_type == "ReadFromStorage" ||
      source_node->node_type == "ReadFromCommonBuffer") {
    return true;
  }
  reason = "leaf resolves to non-scan node type '" + source_node->node_type + "'";
  return false;
}

void validateExpressionTree(const ExpressionNode& root,
                            const std::string& root_label,
                            const std::map<std::string, PlanNode*>& by_id,
                            std::vector<std::string>& failures) {
  struct Frame {
    const ExpressionNode* node = nullptr;
    bool exiting = false;
    bool traversed_children = false;
  };

  std::vector<Frame> stack;
  stack.push_back(Frame{.node = &root, .exiting = false, .traversed_children = false});

  std::set<const ExpressionNode*> active_path;

  while (!stack.empty()) {
    auto frame = stack.back();
    stack.pop_back();
    if (frame.node == nullptr) {
      continue;
    }

    const auto* current = frame.node;
    if (frame.exiting) {
      if (!frame.traversed_children) {
        std::string reason;
        if (!isAcceptableLeaf(*current, by_id, reason)) {
          failures.push_back(root_label + ": invalid leaf " + expressionNodeSummary(*current) + " (" + reason + ")");
        }
      }
      active_path.erase(current);
      continue;
    }

    if (active_path.contains(current)) {
      failures.push_back(root_label + ": cycle detected at " + expressionNodeSummary(*current));
      continue;
    }
    active_path.insert(current);

    std::vector<const ExpressionNode*> next_children;
    if (current->references != nullptr) {
      next_children.push_back(current->references.get());
    }
    for (const auto* child : current->children()) {
      if (child != nullptr) {
        next_children.push_back(child);
      }
    }

    stack.push_back(Frame{
      .node = current,
      .exiting = true,
      .traversed_children = !next_children.empty(),
    });

    for (auto it = next_children.rbegin(); it != next_children.rend(); ++it) {
      stack.push_back(Frame{
        .node = *it,
        .exiting = false,
        .traversed_children = false,
      });
    }
  }
}

} // namespace

bool PlanNode::validate(std::string* diagnostics) {
  std::map<std::string, PlanNode*> by_id;
  indexPlanNodeTreeById(*this, by_id);
  std::vector<std::string> failures;

  auto validate_roots = [&](const std::vector<ExpressionNode>& roots, const std::string& label) {
    for (size_t i = 0; i < roots.size(); ++i) {
      validateExpressionTree(roots[i], label + "[" + std::to_string(i) + "]", by_id, failures);
    }
  };

  for (size_t i = 0; i < headers.size(); ++i) {
    if (headers[i].expression == nullptr) {
      continue;
    }
    validateExpressionTree(*headers[i].expression,
                           "node " + node_id + " headers[" + std::to_string(i) + "]",
                           by_id,
                           failures);
  }
  validate_roots(prewhere_filter_expressions, "node " + node_id + " prewhere");
  validate_roots(actions, "node " + node_id + " actions");
  validate_roots(keys, "node " + node_id + " keys");
  validate_roots(sort_columns, "node " + node_id + " sort");
  validate_roots(aggregates, "node " + node_id + " aggregates");
  validate_roots(filter_columns, "node " + node_id + " filter_columns");
  validate_roots(clauses, "node " + node_id + " clauses");
  validate_roots(output_expressions, "node " + node_id + " outputs");

  if (failures.empty()) {
    return true;
  }

  std::ostringstream out;
  out << "PlanNode validation failed for node '" << node_id << "' with " << failures.size() << " issue(s):";
  for (const auto& failure : failures) {
    out << "\n  - " << failure;
  }
  if (diagnostics != nullptr) {
    *diagnostics = out.str();
  }
  return false;
}

std::string PlanNode::renderPrewhere() const {
  if (prewhere_filter_expressions.empty()) {
    return "";
  }

  for (auto expr_it = prewhere_filter_expressions.rbegin();
       expr_it != prewhere_filter_expressions.rend();
       ++expr_it) {
    if (expr_it->kind == ExpressionNode::Kind::FUNCTION) {
      return expr_it->renderSql();
    }
  }

  return prewhere_filter_expressions.back().renderSql();
}

std::unique_ptr<PlanNode> buildResolvedPlanNodeTree(
    const json& plan_json) {
  auto root = buildPlanNodeTreeRecursive(plan_json);
  for (auto& node : root->bottom_up()) {
    wirePlanNode(node);
  }
  normalizeExistsJoinClausePredicatesFromLineage(*root);
  pruneExistsExpressionTrees(*root);
  wireCommonBuffers(*root);
  rewireCommonBufferLineage(*root);
  assignExpressionAliases(*root);
  return root;
}

void indexPlanNodeTreeById(PlanNode& root, std::map<std::string, PlanNode*>& out_by_id) {
  out_by_id.clear();
  for (auto& node : root.depth_first()) {
    if (node.node_id.empty()) {
      continue;
    }
    out_by_id[node.node_id] = &node;
  }
}

} // namespace sql::clickhouse
