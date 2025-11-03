#pragma once
#include "connection_base.h"
#include "explain/column.h"

namespace sql {
struct ForeignKey {
  std::string_view fk_table_name;
  std::vector<std::string_view> fk_columns;
  std::string_view pk_table_name;
  std::vector<std::string_view> pk_columns;
};

struct Table {
  std::string schema_name;
  std::string table_name;
};

Table splitTable(std::string_view table_name);

/**
 * Cleans up an expression by removing unnecessary whitespace, newlines, tabs etc.
 * @param expression Expression to clean
 * @return A better formatted expression
 */
std::string cleanExpression(std::string expression);

/**
 * Remove a specific function from an expression
 * @param expression Expression to clean
 * @param function_name The function  to remove
 * @return A better formatted expression
 */

std::string removeExpressionFunction(const std::string& expression, std::string_view function_name);
}

