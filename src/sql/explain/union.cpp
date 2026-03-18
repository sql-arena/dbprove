#include "union.h"
#include "group_by.h"
#include "projection.h"
static constexpr const char* symbol_ = "∪";

namespace sql::explain {
namespace {
bool isSimpleIdentifierLocal(const std::string& token) {
  if (token.empty()) {
    return false;
  }
  const auto is_start = [](const char c) {
    return std::isalpha(static_cast<unsigned char>(c)) || c == '_';
  };
  const auto is_char = [](const char c) {
    return std::isalnum(static_cast<unsigned char>(c)) || c == '_';
  };
  if (!is_start(token.front())) {
    return false;
  }
  return std::ranges::all_of(token.begin() + 1, token.end(), is_char);
}

std::vector<std::string> preferredUnionOutputColumns(const Union& node) {
  if (node.isRoot()) {
    return {};
  }
  const auto& parent = node.parent();
  if (parent.type == NodeType::GROUP_BY) {
    const auto* group = dynamic_cast<const GroupBy*>(&parent);
    if (group == nullptr) {
      return {};
    }
    const auto& keys = group->synthetic_group_keys.empty() ? group->group_keys : group->synthetic_group_keys;
    std::vector<std::string> cols;
    cols.reserve(keys.size());
    for (const auto& key : keys) {
      if (key.hasAlias()) {
        cols.push_back(key.alias);
      } else {
        cols.push_back(key.name);
      }
    }
    return cols;
  }
  if (parent.type == NodeType::PROJECTION) {
    const auto* projection = dynamic_cast<const Projection*>(&parent);
    if (projection == nullptr) {
      return {};
    }
    const auto& columns = projection->synthetic_columns_projected.empty()
                            ? projection->columns_projected
                            : projection->synthetic_columns_projected;
    std::vector<std::string> cols;
    cols.reserve(columns.size());
    for (const auto& column : columns) {
      if (column.hasAlias()) {
        cols.push_back(column.alias);
      } else {
        cols.push_back(column.name);
      }
    }
    return cols;
  }
  return {};
}
} // namespace

std::string Union::compactSymbolic() const {
  auto result = std::string(symbol_) + "{";
  if (type == Type::DISTINCT) {
    result += "distinct";
  } else {
    result += "all";
  }

  result += "}";
  return result;
}

std::string Union::renderMuggle(size_t max_width) const {
  std::string result = "UNION ";
  result += to_string(type);
  return result;
}

std::string Union::treeSQLImpl(size_t indent) const {
  std::string result = newline(indent);
  result += "(SELECT * ";
  result += newline(indent);
  result += "FROM (";
  const auto union_keyword = type == Type::ALL ? "UNION ALL" : "UNION DISTINCT";
  const auto& child_nodes = children();
  const auto preferred_output_columns = preferredUnionOutputColumns(*this);
  for (size_t i = 0; i < child_nodes.size(); ++i) {
    result += newline(indent + 1);
    result += "SELECT ";
    if (preferred_output_columns.empty()) {
      result += "* ";
    } else {
      const auto child_alias = child_nodes[i]->subquerySQLAlias();
      for (size_t c = 0; c < preferred_output_columns.size(); ++c) {
        if (c > 0) {
          result += ", ";
        }
        const auto& col = preferred_output_columns[c];
        if (isSimpleIdentifierLocal(col)) {
          result += child_alias + "." + col + " AS " + col;
        } else {
          result += col;
        }
      }
      result += " ";
    }
    result += newline(indent + 1);
    result += "FROM " + child_nodes[i]->treeSQL(indent + 2);
    if (i + 1 < child_nodes.size()) {
      result += newline(indent + 1);
      result += union_keyword;
    }
  }
  result += newline(indent);
  result += ") AS " + subquerySQLAlias() + "_INPUT";
  result += newline(indent);
  result += ") AS " + subquerySQLAlias();
  return result;
}

std::string_view to_string(const Union::Type type) {
  return magic_enum::enum_name(type);
}
}
