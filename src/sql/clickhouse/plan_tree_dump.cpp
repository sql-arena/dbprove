#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

#include "plan_node.h"

namespace fs = std::filesystem;
using nlohmann::json;

namespace {
using namespace sql::clickhouse;

std::string readFile(const fs::path& file) {
  std::ifstream in(file);
  if (!in.is_open()) {
    throw std::runtime_error("Could not open file: " + file.string());
  }
  return std::string(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
}

std::string normalizeQ(std::string value) {
  for (auto& c : value) {
    c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
  }
  if (value.starts_with("TPCH-")) {
    value = value.substr(std::string("TPCH-").size());
  }
  if (value.size() == 2 && value[0] == 'Q' && std::isdigit(static_cast<unsigned char>(value[1]))) {
    value = "Q0" + value.substr(1);
  }
  return value;
}

fs::path resolveArtifactPath(const std::string& input, const fs::path& artifacts_dir) {
  const fs::path as_path(input);
  if (fs::exists(as_path)) {
    return as_path;
  }

  const auto q = normalizeQ(input);
  const auto tpch = artifacts_dir / ("TPCH-" + q + ".json");
  if (fs::exists(tpch)) {
    return tpch;
  }

  const auto plain = artifacts_dir / (input + ".json");
  if (fs::exists(plain)) {
    return plain;
  }

  throw std::runtime_error("Could not resolve artifact from input '" + input + "'");
}

std::string renderExpressionNodeSummary(const ExpressionNode& expression) {
  auto kindLabel = [&]() {
    switch (expression.kind) {
      case ExpressionNode::Kind::UNKNOWN:
        return std::string("UNKNOWN");
      case ExpressionNode::Kind::REFERENCE:
        return std::string("REFERENCE");
      case ExpressionNode::Kind::FUNCTION:
        return expression.function_name.empty() ? std::string("FUNCTION") : std::string("FUNCTION:") + expression.function_name;
      case ExpressionNode::Kind::ALIAS:
        return std::string("ALIAS");
      case ExpressionNode::Kind::COLUMN:
        return std::string("COLUMN");
      case ExpressionNode::Kind::INPUT:
        return std::string("INPUT");
    }
    return std::string("UNKNOWN");
  };
  std::string out;
  if (expression.leaf_binding == ExpressionNode::LeafBinding::CHILD_OUTPUT &&
      !expression.linked_child_output_name.empty()) {
    out = expression.linked_child_output_name;
  } else if (expression.kind == ExpressionNode::Kind::REFERENCE) {
    out = expression.source_name.empty() ? "<ref>" : expression.source_name;
  } else if (expression.kind == ExpressionNode::Kind::FUNCTION) {
    out = expression.function_name.empty() ? "<function>" : expression.function_name;
  } else if (!expression.source_name.empty()) {
    out = expression.source_name;
  } else if (!expression.result_name.empty()) {
    out = expression.result_name;
  } else {
    out = "<expr>";
  }
  std::string linked_node_id = expression.linked_child_node_id;
  std::string linked_output_name = expression.linked_child_output_name;
  if ((linked_node_id.empty() && linked_output_name.empty()) && expression.references != nullptr) {
    linked_node_id = expression.references->plan_node_id;
    linked_output_name = expression.references->output_name;
  }
  if ((linked_node_id.empty() && linked_output_name.empty()) &&
      expression.plan_node_id.empty() &&
      expression.kind == ExpressionNode::Kind::REFERENCE &&
      expression.childCount() == 1 &&
      expression.firstChild() != nullptr) {
    linked_node_id = expression.firstChild()->linked_child_node_id;
    linked_output_name = expression.firstChild()->linked_child_output_name;
  }

  if (!linked_node_id.empty() || !linked_output_name.empty()) {
    out += " -> ";
    if (!linked_node_id.empty()) {
      out += linked_node_id;
      out += ".";
    }
    out += linked_output_name;
    out += " [" + kindLabel() + "]";
    return out;
  }

  if (!expression.plan_node_id.empty()) {
    std::string local_name = expression.output_name;
    if (local_name.empty()) {
      local_name = expression.result_name;
    }
    if (local_name.empty()) {
      local_name = expression.source_name;
    }
    out += " -> ";
    out += expression.plan_node_id;
    if (!local_name.empty()) {
      out += ".";
      out += local_name;
    }
  }
  out += " [" + kindLabel() + "]";
  return out;
}

void printExpressionNodeTree(const ExpressionNode& root,
                             const std::string& prefix = "",
                             std::set<const ExpressionNode*>* active_path = nullptr) {
  std::set<const ExpressionNode*> local_active_path;
  if (active_path == nullptr) {
    active_path = &local_active_path;
  }

  if (active_path->contains(&root)) {
    std::cout << prefix << "- <cycle> " << renderExpressionNodeSummary(root) << "\n";
    return;
  }
  active_path->insert(&root);

  std::cout << prefix << "- " << renderExpressionNodeSummary(root) << "\n";

  if (root.references != nullptr) {
    std::cout << prefix << "  ref:\n";
    printExpressionNodeTree(*root.references, prefix + "    ", active_path);
  } else if (root.linked_child_output_root != nullptr) {
    std::cout << prefix << "  ref:\n";
    printExpressionNodeTree(*root.linked_child_output_root, prefix + "    ", active_path);
  }

  std::vector<const ExpressionNode*> children;
  children.reserve(root.childCount());
  for (const auto* child : root.children()) {
    if (child != nullptr) {
      children.push_back(child);
    }
  }
  for (const auto* child : children) {
    printExpressionNodeTree(*child, prefix + "  ", active_path);
  }

  active_path->erase(&root);
}

void printPlanTree(PlanNode& root) {
  for (auto& node : root.depth_first()) {
    const std::string indent(node.depth() * 2, ' ');
    std::cout << indent << node.node_type << " [" << node.node_id << "]\n";

    if (!node.headers.empty()) {
      std::cout << indent << "  headers:\n";
      for (const auto& h : node.headers) {
        if (h.expression != nullptr && h.expression->kind != ExpressionNode::Kind::UNKNOWN) {
          printExpressionNodeTree(*h.expression, indent + "    ");
          continue;
        }
        ExpressionNode unresolved_header;
        unresolved_header.kind = ExpressionNode::Kind::COLUMN;
        unresolved_header.plan_node_id = node.node_id;
        unresolved_header.source_name = h.name;
        unresolved_header.output_name = h.name;
        printExpressionNodeTree(unresolved_header, indent + "    ");
      }
    }

    if (!node.prewhere_filter_expressions.empty()) {
      std::cout << indent << "  prewhere:\n";
      for (const auto& prewhere_root : node.prewhere_filter_expressions) {
        printExpressionNodeTree(prewhere_root, indent + "    ");
      }
    }

    if (!node.unresolved_keys.empty() || !node.keys.empty()) {
      std::cout << indent << "  keys:\n";
      for (size_t i = 0; i < node.unresolved_keys.size(); ++i) {
        ExpressionNode unresolved_key;
        unresolved_key.kind = ExpressionNode::Kind::COLUMN;
        unresolved_key.plan_node_id = node.node_id;
        unresolved_key.source_name = node.unresolved_keys[i];
        unresolved_key.output_name = node.unresolved_keys[i];
        printExpressionNodeTree(unresolved_key, indent + "    ");
        if (i < node.keys.size()) {
          printExpressionNodeTree(node.keys[i], indent + "      ");
        }
      }
    }

    if (!node.actions.empty()) {
      std::cout << indent << "  actions:\n";
      for (const auto& action : node.actions) {
        printExpressionNodeTree(action, indent + "    ");
      }
    }

    if (!node.aggregates.empty()) {
      std::cout << indent << "  aggregates:\n";
      for (const auto& aggregate : node.aggregates) {
        printExpressionNodeTree(aggregate, indent + "    ");
      }
    }

    if (!node.unresolved_clauses.empty() || !node.clauses.empty()) {
      std::cout << indent << "  clauses:\n";
      if (!node.unresolved_clauses.empty()) {
        ExpressionNode unresolved_clause;
        unresolved_clause.kind = ExpressionNode::Kind::COLUMN;
        unresolved_clause.plan_node_id = node.node_id;
        unresolved_clause.source_name = node.unresolved_clauses;
        printExpressionNodeTree(unresolved_clause, indent + "    ");
      }
      for (const auto& clause_expr : node.clauses) {
        printExpressionNodeTree(clause_expr, indent + "    ");
      }
    }

    if (!node.output_expressions.empty()) {
      std::cout << indent << "  outputs:\n";
      for (const auto& output : node.output_expressions) {
        if (output.output_name.empty()) {
          continue;
        }
        printExpressionNodeTree(output, indent + "    ");
      }
    }
  }
}

void printUsage() {
  std::cout
      << "Usage: clickhouse_plan_tree_dump <query_or_artifact> [--artifacts-dir <dir>]\n"
      << "Examples:\n"
      << "  clickhouse_plan_tree_dump Q01\n"
      << "  clickhouse_plan_tree_dump TPCH-Q06\n"
      << "  clickhouse_plan_tree_dump run/artifacts/clickhouse/TPCH-Q12.json\n";
}
} // namespace

int main(int argc, char** argv) {
  try {
    if (argc < 2) {
      printUsage();
      return 1;
    }

    std::string input;
    fs::path artifacts_dir = fs::path("run") / "artifacts" / "clickhouse";

    for (int i = 1; i < argc; ++i) {
      const std::string arg = argv[i];
      if (arg == "--artifacts-dir") {
        if (i + 1 >= argc) {
          throw std::runtime_error("--artifacts-dir requires a value");
        }
        artifacts_dir = argv[++i];
        continue;
      }
      if (arg == "-h" || arg == "--help") {
        printUsage();
        return 0;
      }
      if (input.empty()) {
        input = arg;
      } else {
        throw std::runtime_error("Unexpected extra argument: " + arg);
      }
    }

    if (input.empty()) {
      throw std::runtime_error("Missing query_or_artifact argument");
    }

    const auto artifact_file = resolveArtifactPath(input, artifacts_dir);
    const auto content = readFile(artifact_file);
    const auto explain_json = json::parse(content);
    if (!explain_json.is_array() || explain_json.empty() || !explain_json[0].contains("Plan")) {
      throw std::runtime_error("Invalid explain JSON: missing [0].Plan");
    }

    auto plan_root = buildResolvedPlanNodeTree(explain_json[0]["Plan"]);
    std::cout << "Artifact: " << artifact_file.string() << "\n";
    printPlanTree(*plan_root);
    return 0;
  } catch (const std::exception& e) {
    std::cerr << "ERROR: " << e.what() << "\n";
    return 2;
  }
}
