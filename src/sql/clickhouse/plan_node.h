#pragma once

#include <algorithm>
#include <atomic>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <dbprove/common/tree_node.h>
#include <nlohmann/json.hpp>

#include "expression_node.h"

namespace sql::clickhouse {

struct PlanNodeHeader {
  std::string name;
  std::string type;
  std::unique_ptr<ExpressionNode> expression;
};

struct PlanNode : TreeNode<PlanNode> {
  std::string node_id;
  std::string node_type;
  std::string description;
  nlohmann::json unresolved_prewhere_filter_expression;
  std::vector<ExpressionNode> prewhere_filter_expressions;

  std::vector<PlanNodeHeader> headers;
  nlohmann::json unresolved_actions;
  std::map<int64_t, std::string> unresolved_input_name_by_slot;
  std::vector<ExpressionNode> actions;

  std::vector<std::string> unresolved_keys;
  std::vector<ExpressionNode> keys;
  std::vector<bool> unresolved_sort_ascending;
  std::vector<ExpressionNode> sort_columns;

  std::vector<ExpressionNode> aggregates;

  std::vector<std::string> unresolved_filter_columns;
  std::vector<ExpressionNode> filter_columns;
  std::string unresolved_clauses;
  std::vector<ExpressionNode> clauses;

  std::vector<ExpressionNode> output_expressions;

  PlanNode* common_buffer_producer = nullptr;

  nlohmann::json raw_json;

  bool validate(std::string* diagnostics = nullptr);
  std::string renderPrewhere() const;
};

std::unique_ptr<PlanNode> buildResolvedPlanNodeTree(
    const nlohmann::json& plan_json);

void indexPlanNodeTreeById(PlanNode& root, std::map<std::string, PlanNode*>& out_by_id);

} // namespace sql::clickhouse
