#pragma once

#include <algorithm>
#include <atomic>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include <dbprove/common/tree_node.h>
#include <nlohmann/json.hpp>

namespace sql::clickhouse {

struct ExpressionNode : TreeNode<ExpressionNode> {
  enum class Kind {
    UNKNOWN,
    REFERENCE,
    FUNCTION,
    ALIAS,
    COLUMN,
    INPUT
  };

  enum class LeafBinding {
    NONE,
    BASE_TABLE,
    CHILD_OUTPUT
  };

  Kind kind = Kind::UNKNOWN;
  LeafBinding leaf_binding = LeafBinding::NONE;

  std::string plan_node_id;
  int64_t result_id = -1;
  int64_t result_slot = -1;
  int64_t input_slot = -1;
  std::string output_name;
  std::string output_type;
  std::string result_name;
  std::string expression;
  std::string function_name;
  std::string source_name;
  std::string alias_sql;
  std::string alias_user;
  std::shared_ptr<ExpressionNode> references;

  // Leaf wiring information (pass 2)
  std::string base_table_name;
  std::string linked_child_node_id;
  std::string linked_child_output_name;
  ExpressionNode* linked_child_output_root = nullptr;

  bool isExists() const;
  bool isRealAliased() const;
  void setExpression(std::string value);
  std::string renderSql() const;
  std::string renderSql(const std::map<std::string, std::string>& plan_alias_by_node_id) const;
  std::string renderUser() const;
  std::string renderExecutableSql() const;
  static std::vector<std::string> renderExecutableSqlList(const std::vector<ExpressionNode>& output_expressions);
};

} // namespace sql::clickhouse
