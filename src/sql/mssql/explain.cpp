#include "connection.h"
#include "explain/plan.h"
#include <cctype>
#include <memory>
#include <pugixml.hpp>
#include <regex>
#include <set>
#include <unordered_set>

#include "sql_exceptions.h"
#include <explain_nodes.h>
#include <explain/distribution.h>
#include <explain/materialise.h>
#include <explain/scan_materialised.h>
#include <explain/sequence.h>
#include <plog/Log.h>

#include "mio/mmap.hpp"

using namespace sql::explain;
using namespace pugi;

namespace sql::mssql {
struct MssqlExplainCtx {
  std::unordered_set<std::string> aliases;
};

// Forward declarations
std::pair<std::unique_ptr<Node>, std::vector<xml_node>> createNodeFromXML(const xml_node& node_xml,
                                                                          MssqlExplainCtx& ctx,
                                                                          std::string pushed_filter = "");

/**
 * A custom dialect for SQL Server expressions that is aware of aliases.
 */
struct MssqlDialect final : EngineDialect {
  explicit MssqlDialect(const MssqlExplainCtx& ctx) : ctx_(ctx) {}

  void preRender(std::vector<Token>& tokens) override {
    for (auto& token : tokens) {
      if (token.type == Token::Type::Literal) {
        // Handle qualified names: table.column or alias.column
        const size_t dot_pos = token.value.find('.');
        if (dot_pos != std::string::npos && dot_pos > 0 && dot_pos + 1 < token.value.size()) {
          const std::string qualifier = token.value.substr(0, dot_pos);
          const std::string column = token.value.substr(dot_pos + 1);

          // Only treat identifier-like qualifiers as table/alias qualifiers.
          // This avoids breaking numeric literals such as "1.".
          const bool qualifier_is_identifier =
              std::isalpha(static_cast<unsigned char>(qualifier.front())) || qualifier.front() == '_';
          if (!qualifier_is_identifier) {
            continue;
          }

          // If the qualifier is not an alias, we strip it.
          // Otherwise, we keep it as a two-part name.
          if (!ctx_.aliases.contains(qualifier)) {
            token.value = column;
          }
        }
      }
    }
  }

private:
  const MssqlExplainCtx& ctx_;
};

/**
 * Alias-aware expression cleaner for SQL Server.
 */
std::string cleanMssqlExpression(std::string expression, const MssqlExplainCtx& ctx) {
  MssqlDialect dialect(ctx);
  return cleanExpression(std::move(expression), &dialect);
}

double getTotalActualRows(const xml_node& node_xml) {
  const auto runtime_info = node_xml.child("RunTimeInformation");
  if (!runtime_info) return 0;
  
  double total = 0;
  for (auto per_thread : runtime_info.children("RunTimeCountersPerThread")) {
    total += per_thread.attribute("ActualRows").as_double();
  }
  return total;
}

double getTotalActualExecutions(const xml_node& node_xml) {
  const auto runtime_info = node_xml.child("RunTimeInformation");
  if (!runtime_info) return 0;
  
  double total = 0;
  for (auto per_thread : runtime_info.children("RunTimeCountersPerThread")) {
    total += per_thread.attribute("ActualExecutions").as_double();
  }
  return total;
}

void parseRowCount(Node* node, xml_node node_xml) {
  const double rows_estimated = node_xml.attribute("EstimateRows").as_double();
  node->rows_estimated = rows_estimated;

  const auto runtime_info = node_xml.child("RunTimeInformation");
  if (runtime_info) {
    double total_actual_rows = 0;
    double total_actual_rows_read = 0;
    double total_locally_aggregated_rows = 0;
    bool found_any = false;
    bool found_any_read = false;
    bool found_any_locally_aggregated = false;

    for (auto per_thread : runtime_info.children("RunTimeCountersPerThread")) {
      total_actual_rows += per_thread.attribute("ActualRows").as_double();
      found_any = true;

      const auto actual_rows_read = per_thread.attribute("ActualRowsRead");
      if (actual_rows_read) {
          total_actual_rows_read += actual_rows_read.as_double();
          found_any_read = true;
      }

      const auto locally_aggregated = per_thread.attribute("ActualLocallyAggregatedRows");
      if (locally_aggregated) {
          total_locally_aggregated_rows += locally_aggregated.as_double();
          found_any_locally_aggregated = true;
      }
    }

    const auto rowstore_batch = runtime_info.child("BatchModeOnRowStoreCounters");
    if (rowstore_batch) {
        total_actual_rows += rowstore_batch.attribute("ActualRows").as_double();
        found_any = true;

        const auto actual_rows_read = rowstore_batch.attribute("ActualRowsRead");
        if (actual_rows_read) {
            total_actual_rows_read += actual_rows_read.as_double();
            found_any_read = true;
        }
    }

    // Sometimes SQL Server puts ActualRows directly in RunTimeInformation (e.g. for batch mode hash join)
    const auto actual_rows = runtime_info.attribute("ActualRows");
    if (actual_rows) {
        total_actual_rows = actual_rows.as_double();
        found_any = true;
    }

    const auto actual_rows_read = runtime_info.attribute("ActualRowsRead");
    if (actual_rows_read) {
        total_actual_rows_read = actual_rows_read.as_double();
        found_any_read = true;
    }

    if (found_any) {
      node->rows_actual = total_actual_rows;

      // If we found locally aggregated rows, and ActualRows is 0, we should use those.
      // This happens in Batch Mode scans where the scan itself performs some aggregation.
      if (node->rows_actual == 0 && found_any_locally_aggregated && total_locally_aggregated_rows > 0) {
          node->rows_actual = total_locally_aggregated_rows;
      }

      // For scan nodes, we prefer to show the number of rows read if it's available and larger than rows produced.
      // This is because SQL Server often does partial aggregation in the scan, but we want to know the table size.
      if ((node->type == NodeType::SCAN || node->type == NodeType::SCAN_MATERIALISED) && found_any_read && total_actual_rows_read > node->rows_actual) {
          node->rows_actual = total_actual_rows_read;
      }
    }
  }
}

/**
 * SQL Server, for some reason, doesn't always add actual row counts to projections.
 * But these cannot change the actual value, so we can correct this by simply taking the count of the child.
 * @param node
 */
void fixRowCounts(Node* node) {
  for (auto& n : node->bottom_up()) {
    // If the node already has non-zero rows, we generally trust it.
    if (n.type == NodeType::MATERIALISE) {
        // For Materialise nodes, we ALWAYS prefer the child's row count for actuals and estimates.
        // SQL Server reports cumulative rows for spools in loops, which is misleading
        // for a materialization step.
        if (n.childCount() > 0) {
            n.rows_actual = n.firstChild()->rows_actual;
            n.rows_estimated = n.firstChild()->rows_estimated;
        }
    } else if (!std::isinf(n.rows_actual) && !std::isnan(n.rows_actual) && n.rows_actual > 0) {
      // Still need to check estimated rows if they are missing
    } else if (n.childCount() > 0) {
      // Propagate actual rows
      const Node* source_child = (n.type == NodeType::SEQUENCE) ? n.lastChild() : n.firstChild();
      const double child_rows = source_child->rows_actual;
      if (!std::isinf(child_rows) && !std::isnan(child_rows) && child_rows > 0) {
        // We only propagate for nodes that are "transparent" (don't filter or expand rows themselves).
        // Note: FILTER is NOT included here because it's meant to reduce row counts.
        if (n.type == NodeType::PROJECTION ||
            n.type == NodeType::SORT || 
            n.type == NodeType::LIMIT || 
            n.type == NodeType::DISTRIBUTE ||
            n.type == NodeType::SEQUENCE) {
          n.rows_actual = child_rows;
        }
      }
    }

    // Also propagate estimated rows if they are missing
    if (std::isnan(n.rows_estimated) || n.rows_estimated <= 0) {
      if (n.childCount() > 0) {
        const Node* source_child = (n.type == NodeType::SEQUENCE) ? n.lastChild() : n.firstChild();
        const double child_est = source_child->rows_estimated;
        if (!std::isnan(child_est) && child_est > 0) {
           if (n.type == NodeType::PROJECTION ||
               n.type == NodeType::SORT || 
               n.type == NodeType::LIMIT || 
               n.type == NodeType::DISTRIBUTE ||
               n.type == NodeType::SEQUENCE) {
             n.rows_estimated = child_est;
           }
        }
      }
    }
  }
}

/**
 * SQL Server uses a Nested Loops join to trigger a spool (materialization).
 * This transformation hoists these materializations to a new root Sequence node.
 */
void hoistMaterialisations(std::unique_ptr<Node>& root) {
  std::vector<std::shared_ptr<Node>> hoisted;
  
  // Collect all ScanMaterialised node IDs to see which Materialise nodes have consumers
  std::unordered_set<int> consumer_ids;
  for (const auto& n : root->depth_first()) {
    if (n.type == NodeType::SCAN_MATERIALISED) {
      const auto scan = static_cast<const ScanMaterialised*>(&n);
      if (scan->primary_node_id >= 0) {
        consumer_ids.insert(scan->primary_node_id);
      }
    }
  }

  // We need to find joins that have a Materialise child
  // We iterate bottom-up to handle nested joins if they exist
  for (auto& n : root->bottom_up()) {
    if (n.type == NodeType::JOIN) {
      Join* join = static_cast<Join*>(&n);
      Node* to_hoist = nullptr;
      size_t hoist_index = 0;
      
      for (size_t i = 0; i < join->childCount(); ++i) {
        if (join->children()[i]->type == NodeType::MATERIALISE) {
          const auto materialise = static_cast<const Materialise*>(join->children()[i]);
          // Only hoist if there is a consumer for this materialisation
          if (consumer_ids.contains(materialise->node_id)) {
            to_hoist = join->children()[i];
            hoist_index = i;
            break;
          }
        }
      }
      
      if (to_hoist) {
        // Take the Materialise node out of the join
        auto materialise = join->takeChild(hoist_index);
        hoisted.push_back(std::move(materialise));
        
        // If the join now has only one child, we should replace the join with that child
        if (join->childCount() == 1) {
            auto remaining_child = join->takeChild(0);
            if (join->isRoot()) {
                join->addSharedChild(std::move(remaining_child));
            } else {
                join->replaceWithShared(std::move(remaining_child));
            }
        }
      }
    }
  }
  
  if (!hoisted.empty()) {
      auto sequence = std::make_unique<Sequence>();
      for (auto& h : hoisted) {
          sequence->addSharedChild(std::move(h));
      }
      
      // The original root (possibly modified) becomes the last child of the sequence
      sequence->addChild(std::move(root));
      root = std::move(sequence);
      root->setParentToSelf();
  }
}

const std::regex bloom_regex(R"(PROBE\(\[*Opt_Bitmap[0-9]+\]*,(.*)\))");
const std::regex bracket_regex(R"(\[|\])");

static std::string qname(const xml_node& column_reference, const MssqlExplainCtx& ctx) {
  auto get = [&](const char* a)-> std::string {
    auto v = column_reference.attribute(a);
    return v ? v.value() : "";
  };
  const std::string table = get("Table");
  const std::string alias = get("Alias");
  const std::string column = get("Column");
  
  if (!alias.empty()) {
    return alias + "." + column;
  }
  
  // No alias, so we just want the column name.
  return column;
}

std::string cleanFilter(std::string filter, const MssqlExplainCtx& ctx) {
  size_t old_size = 0;
  do {
    old_size = filter.size();
    filter = std::regex_replace(filter, bloom_regex, "BLOOM($1)");
    filter = std::regex_replace(filter, bracket_regex, "");
    
    // Only strip schema/database prefixes like [db].[schema]. or schema.table.
    // We want to keep Alias.Column or Table.Column.
    // Use the same consistent logic as cleanExpression.
    static const std::regex schema_prefix_regex(R"(([a-zA-Z_]\w*\.){2,})");
    filter = std::regex_replace(filter, schema_prefix_regex, "");
  } while (old_size != filter.size());
  return cleanMssqlExpression(filter, ctx);
}

std::string deduplicateFilters(const std::string& filter, const MssqlExplainCtx& ctx) {
  if (filter.empty()) return filter;
  
  std::vector<std::string> parts;
  std::string current;
  int paren_count = 0;
  
  auto push_part = [&](std::string p) {
      // Trim
      p.erase(0, p.find_first_not_of(" \t\n\r"));
      p.erase(p.find_last_not_of(" \t\n\r") + 1);
      if (!p.empty()) {
          parts.push_back(p);
      }
  };

  for (size_t i = 0; i < filter.size(); ++i) {
      if (filter[i] == '(') paren_count++;
      else if (filter[i] == ')') paren_count--;
      
      if (paren_count == 0 && i + 5 <= filter.size() && filter.substr(i, 5) == " AND ") {
          push_part(current);
          current = "";
          i += 4; // skip " AND"
      } else {
          current += filter[i];
      }
  }
  push_part(current);
  
  if (parts.size() <= 1) return filter;
  
  std::vector<std::string> unique_parts;
  std::set<std::string> seen;
  for (const auto& p : parts) {
      std::string clean = cleanFilter(p, ctx);
      if (seen.find(clean) == seen.end()) {
          unique_parts.push_back(p);
          seen.insert(clean);
      }
  }
  
  if (unique_parts.size() == parts.size()) return filter;
  
  std::string result;
  for (size_t i = 0; i < unique_parts.size(); ++i) {
      result += unique_parts[i];
      if (i < unique_parts.size() - 1) {
          result += " AND ";
      }
  }
  return result;
}

static std::string scalarOperatorToExpression(const xml_node& scalar_op, const MssqlExplainCtx& ctx) {
  if (!scalar_op) return "";
  
  // From the user: "RelOp actually contains the alias. Instead of using the scalarString, 
  // you could parse the actual ScalarOperator into an expression"
  
  // Priority 1: If it's a direct ColumnReference, use qname to get alias.column
  const auto column_ref = scalar_op.child("ColumnReference");
  if (column_ref) {
      return qname(column_ref, ctx);
  }

  // SQL Server often wraps identifiers as ScalarOperator -> Identifier -> ColumnReference.
  const auto identifier = scalar_op.child("Identifier");
  if (identifier) {
    const auto id_col_ref = identifier.child("ColumnReference");
    if (id_col_ref) {
      return qname(id_col_ref, ctx);
    }
  }
  
  // Priority 2: Recurse into common structures if they exist (handling only simple ones for now)
  // Most SQL Server expressions in plans are nested ScalarOperators
  const auto nested_scalar = scalar_op.child("ScalarOperator");
  if (nested_scalar) {
      return scalarOperatorToExpression(nested_scalar, ctx);
  }

  // Fallback: use the stringified version provided by SQL Server, but clean it.
  return cleanFilter(scalar_op.attribute("ScalarString").as_string(), ctx);
}

std::string definedValueExpression(const xml_node& defined_value, const MssqlExplainCtx& ctx) {
  const auto scalar_op = defined_value.child("ScalarOperator");
  std::string name;

  // SQL Server uses ANY(...) internally in aggregate defined values for pass-through columns.
  // We should render the underlying expression/column, never "IN ...".
  if (scalar_op) {
    const auto aggregate = scalar_op.child("Aggregate");
    if (aggregate) {
      const auto agg_type = to_upper(std::string(aggregate.attribute("AggType").as_string()));
      if (agg_type == "ANY") {
        for (auto arg_scalar : aggregate.children("ScalarOperator")) {
          name = scalarOperatorToExpression(arg_scalar, ctx);
          if (!name.empty()) {
            break;
          }
        }
        if (name.empty()) {
          const auto agg_col_ref = aggregate.child("ColumnReference");
          if (agg_col_ref) {
            name = qname(agg_col_ref, ctx);
          }
        }
      }
    }
  }

  if (name.empty()) {
    name = scalarOperatorToExpression(scalar_op, ctx);
  }
  const auto column_ref = defined_value.child("ColumnReference");
  if (column_ref) {
    const std::string alias = column_ref.attribute("Column").as_string();
    if (name != alias && name != cleanFilter(alias, ctx)) {
      name += " AS " + alias;
    }
  }

  return name;
}

static std::string findSeekCondition(const xml_node& inner_rel_op, const MssqlExplainCtx& ctx) {
  auto seek_predicates = inner_rel_op.select_nodes(".//*[local-name()='SeekPredicates']");

  if (seek_predicates.empty()) {
    seek_predicates = inner_rel_op.select_nodes(".//*[local-name()='SeekPredicateNew']");
  }
  if (seek_predicates.empty()) {
    seek_predicates = inner_rel_op.select_nodes(".//*[local-name()='Predicate' ]");
  }
  for (auto sp : seek_predicates) {
    const auto scalar_op = sp.node().child("ScalarOperator");
    if (scalar_op) {
      return scalarOperatorToExpression(scalar_op, ctx);
    }
  }

  /* Failed to dind the old style predicate, the new ones are a form of AST, so go after those instead */
  const auto seek_predicate_news = inner_rel_op.select_nodes(".//*[local-name()='SeekPredicateNew']");

  std::vector<xml_node> innerCols;
  std::vector<xml_node> outerCols;

  for (auto sp : seek_predicate_news) {
    const auto spNode = sp.node();
    for (auto n : spNode.select_nodes(".//*[local-name()='RangeColumns']/*[local-name()='ColumnReference']")) {
      innerCols.push_back(n.node());
    }
    for (auto n : spNode.select_nodes(".//*[local-name()='Identifier']/*[local-name()='ColumnReference']")) {
      outerCols.push_back(n.node());
    }
  }

  // Zip them by position; typical plans align these 1:1 for equi-join prefixes
  std::string condition;
  size_t m = std::min(innerCols.size(), outerCols.size());

  for (size_t i = 0; i < m; ++i) {
    std::string innerQN = qname(innerCols[i], ctx);
    std::string outerQN = qname(outerCols[i], ctx);
    condition += outerQN + " = " + innerQN;
    if (i < m - 1) {
      condition += " AND ";
    }
  }

  return condition;
}


bool isScanOp(const std::string& physical_op) {
  const std::set<std::string> scan_ops = {"Clustered Index Scan", "Clustered Index Seek", "Index Scan", "Index Seek", "Table Scan"};
  return scan_ops.contains(physical_op);
}

std::unique_ptr<Node> handleTop(const xml_node& node_xml, std::vector<xml_node>& children) {
  const auto top = node_xml.child("Top");
  const auto child = top.child("RelOp");
  auto top_count = top.attribute("RowCount").as_llong();
  children.push_back(child);
  return std::make_unique<Limit>(top_count);
}

std::unique_ptr<Node> handleSpool(const xml_node& node_xml, const std::string& physical_op, std::vector<xml_node>& children) {
  xml_node child = node_xml.child("RelOp");
  int primary_node_id = -1;
  int node_id = -1;

  auto node_id_attr = node_xml.attribute("NodeId");
  if (node_id_attr) {
    node_id = node_id_attr.as_int();
  }

  // Try to find PrimaryNodeId which indicates this is a consumer of a spool
  for (auto sub : node_xml.children()) {
    const std::string name = sub.name();
    if (name.find("Spool") != std::string::npos) {
        auto primary_attr = sub.attribute("PrimaryNodeId");
        if (primary_attr) {
            primary_node_id = primary_attr.as_int();
        }
        if (!child) {
          child = sub.child("RelOp");
        }
        if (primary_node_id != -1 && child) break;
    }
  }

  if (primary_node_id != -1) {
    // This is a consumer (reading from a previously created spool)
    // Even if it has a child (unlikely for a consumer), we ignore it or it's part of the producer part.
    // In SQL Server plans, a Spool with a PrimaryNodeId usually has no child RelOp.
    return std::make_unique<ScanMaterialised>(primary_node_id);
  }

  // This is a producer (creating the spool)
  if (child) {
    children.push_back(child);
  }

  // We can use physical_op as the name for the materialization
  return std::make_unique<Materialise>(physical_op, node_id);
}

std::unique_ptr<Node> handleFilter(const xml_node& node_xml, MssqlExplainCtx& ctx, std::vector<xml_node>& children, std::string& pushed_filter) {
  const auto filter = node_xml.child("Filter");
  const auto child = filter.child("RelOp");
  const auto child_op = std::string(child.attribute("PhysicalOp").as_string());
  const auto predicate = filter.child("Predicate");
  std::string condition;
  if (predicate) {
    const auto scalar_op = predicate.child("ScalarOperator");
    if (scalar_op) {
      condition = scalarOperatorToExpression(scalar_op, ctx);
    }
  }
  if (isScanOp(child_op)) {
    // We are calculating bloom filters here. Get rid of those
    pushed_filter = std::regex_replace(condition, bloom_regex, "");
    
    // We don't return nullptr anymore. Instead, we manually build the child node
    // and transfer the actual rows from the filter node to the scan node if needed.
    auto result = createNodeFromXML(child, ctx, pushed_filter);
    if (result.first->rows_actual <= 0) {
        parseRowCount(result.first.get(), node_xml);
    }
    
    // Important: we have ALREADY added the filter to the child (via pushed_filter in createNodeFromXML -> handleScan)
    // and we have ALREADY parsed the row count for the child (possibly using THIS node_xml).
    // Returning result.first is correct, but we must ensure that the recursive call 
    // to buildExplainNode doesn't add the child again.
    // handleFilter doesn't add the child to 'children' vector in this branch, so it's safe.
    return std::move(result.first);
  }
  MssqlDialect dialect(ctx);
  condition = cleanFilter(condition, ctx);
  children.push_back(filter.child("RelOp"));
  auto selection = std::make_unique<Selection>(condition, &dialect);
  return selection;
}

std::unique_ptr<Node> handleSort(const xml_node& node_xml, const MssqlExplainCtx& ctx, std::vector<xml_node>& children) {
  std::vector<Column> columns_sorted;
  auto sort = node_xml.child("Sort");
  if (!sort) {
    // The heap sorting
    sort = node_xml.child("TopSort");
  }
  MssqlDialect dialect(ctx);
  for (auto column : sort.child("OrderBy")) {
    const Column::Sorting sorting = column.attribute("Ascending").as_bool()
                                      ? Column::Sorting::ASC
                                      : Column::Sorting::DESC;
    const auto name = std::string(column.child("ColumnReference").attribute("Column").as_string());
    columns_sorted.push_back(Column(name, sorting, &dialect));
  }
  children.push_back(sort.child("RelOp"));
  return std::make_unique<Sort>(columns_sorted);
}

std::unique_ptr<Node> handleSegment(const xml_node& node_xml, const MssqlExplainCtx& ctx, std::vector<xml_node>& children) {
  std::vector<Column> columns_grouped;
  const auto segment = node_xml.child("Segment");
  const auto child_rel_op = segment.child("RelOp");

  const auto group_by = segment.child("GroupBy");
  MssqlDialect dialect(ctx);
  for (auto column_ref : group_by.children("ColumnReference")) {
    const auto name = qname(column_ref, ctx);
    if (!name.empty()) {
        columns_grouped.push_back(Column(name, &dialect));
    }
  }

  children.push_back(child_rel_op);
  
  GroupBy::Strategy strategy = GroupBy::Strategy::SORT_MERGE;
  if (child_rel_op) {
      const std::string child_op = child_rel_op.attribute("PhysicalOp").as_string();
      if (child_op.find("Hash") != std::string::npos) {
          strategy = GroupBy::Strategy::HASH;
      }
  }
  
  if (columns_grouped.empty()) {
    strategy = GroupBy::Strategy::SIMPLE;
  }

  return std::make_unique<GroupBy>(strategy, columns_grouped, std::vector<Column>{});
}

std::unique_ptr<Node> handleAggregate(const xml_node& node_xml, const MssqlExplainCtx& ctx, const std::string& physical_op, std::vector<xml_node>& children) {
  std::vector<Column> columns_grouped;
  std::vector<Column> columns_aggregated;

  GroupBy::Strategy strategy = (physical_op == "Hash Match") ? GroupBy::Strategy::HASH : GroupBy::Strategy::SORT_MERGE;
  MssqlDialect dialect(ctx);

  if (physical_op == "Hash Match") {
    const auto hash_child = node_xml.child("Hash");
    for (auto defined_value : hash_child.child("DefinedValues")) {
      auto name = definedValueExpression(defined_value, ctx);
      columns_aggregated.push_back(Column(name, &dialect));
    }
    auto hash_keys = hash_child.child("HashKeysBuild");
    if (!hash_keys) {
      hash_keys = hash_child.child("HashKeysProbe");
    }
    for (auto column_ref : hash_keys.children("ColumnReference")) {
      const auto name = qname(column_ref, ctx);
      columns_grouped.push_back(Column(name, &dialect));
    }
    children.push_back(hash_child.child("RelOp"));
  } else if (physical_op == "Stream Aggregate") {
    const auto stream_aggregate_child = node_xml.child("StreamAggregate");
    for (auto defined_value : stream_aggregate_child.child("DefinedValues")) {
      auto name = definedValueExpression(defined_value, ctx);
      columns_aggregated.push_back(Column(name, &dialect));
    }
    for (auto column_ref : stream_aggregate_child.child("GroupBy").children("ColumnReference")) {
      const auto name = qname(column_ref, ctx);
      columns_grouped.push_back(Column(name, &dialect));
    }
    children.push_back(stream_aggregate_child.child("RelOp"));
  }

  if (columns_grouped.empty()) {
    strategy = GroupBy::Strategy::SIMPLE;
  }

  return std::make_unique<GroupBy>(strategy, columns_grouped, columns_aggregated);
}

std::string joinConditionFromKeys(const xml_node& build_keys, const xml_node& probe_keys, const MssqlExplainCtx& ctx) {
  std::vector<xml_node> buildCols;
  std::vector<xml_node> probeCols;

  for (auto n : build_keys.children("ColumnReference")) {
    buildCols.push_back(n);
  }
  for (auto n : probe_keys.children("ColumnReference")) {
    probeCols.push_back(n);
  }

  std::string condition;
  size_t m = std::min(buildCols.size(), probeCols.size());

  for (size_t i = 0; i < m; ++i) {
    condition += qname(probeCols[i], ctx) + " = " + qname(buildCols[i], ctx);
    if (i < m - 1) {
      condition += " AND ";
    }
  }

  return condition;
}

std::unique_ptr<Node> handleJoin(const xml_node& node_xml, const MssqlExplainCtx& ctx, const std::string& physical_op, const std::string& logical_op, std::vector<xml_node>& children) {
  if (logical_op == "Inner Join" && physical_op == "Merge Join") {
    const auto merge = node_xml.child("Merge");
    std::string condition;
    
    // Prioritize Residual if it exists, as it often contains the full join condition
    const auto residual = merge.child("Residual");
    if (residual) {
      const auto scalar_operator = residual.child("ScalarOperator");
      condition = scalarOperatorToExpression(scalar_operator, ctx);
    } else {
      const auto inner_keys = merge.child("InnerSideJoinColumns");
      const auto outer_keys = merge.child("OuterSideJoinColumns");
      if (inner_keys && outer_keys) {
        condition = joinConditionFromKeys(inner_keys, outer_keys, ctx);
      }
    }
    
    for (auto child : merge.children("RelOp")) {
      children.push_back(child);
    }
    return std::make_unique<Join>(Join::Type::INNER, Join::Strategy::MERGE, condition);
  }

  if (physical_op == "Hash Match" || physical_op == "Adaptive Join") {
    const auto hash = node_xml.child(physical_op == "Hash Match" ? "Hash" : "AdaptiveJoin");
    std::string condition;

    // Prioritize ProbeResidual if it exists, as it often contains the full join condition (including equi-keys)
    const auto probe = hash.child("ProbeResidual");
    if (probe) {
      const auto scalar_operator = probe.child("ScalarOperator");
      condition = scalarOperatorToExpression(scalar_operator, ctx);
    } else {
      const auto build_keys = hash.child("HashKeysBuild");
      const auto probe_keys = hash.child("HashKeysProbe");
      if (build_keys && probe_keys) {
        condition = joinConditionFromKeys(build_keys, probe_keys, ctx);
      }
    }
    
    // SQL Server Join operators (especially Adaptive Join) can contain multiple child branches, 
    // some of which might not have been executed during runtime (e.g., alternative strategies).
    // For Adaptive Join, we try to prune. For standard Hash Join, we take first two.
    if (physical_op == "Adaptive Join") {
        for (auto child : hash.children("RelOp")) {
          if (getTotalActualExecutions(child) > 0 || getTotalActualRows(child) > 0) {
              children.push_back(child);
          }
        }
    }
    
    // Fallback: if we still have no children, or it's a standard Hash Join, take the first two RelOps.
    if (children.empty()) {
        int count = 0;
        for (auto child : hash.children("RelOp")) {
          children.push_back(child);
          if (++count == 2) break;
        }
    }

    return std::make_unique<Join>(Join::Type::INNER, Join::Strategy::HASH, condition);
  }

  if (physical_op == "Nested Loops") {
    const auto join_type = Join::Type::INNER;
    const auto nested_loops = node_xml.child("NestedLoops");

    std::vector<xml_node> nl_children;
    for (auto child : nested_loops.children("RelOp")) {
        nl_children.push_back(child);
    }
    
    if (nl_children.size() == 2) {
      // Loop joins are the wrong way around in SQL Server plans
      // inner is nl_children[1], outer is nl_children[0] before reverse
      // After reverse, inner is first.
      std::ranges::reverse(nl_children);
    }

    const auto logical_join_type = Join::typeFromString(logical_op);
    for (auto child : nl_children) {
      if (logical_join_type == Join::Type::LEFT_ANTI) {
        const auto child_op_type = std::string(child.attribute("PhysicalOp").as_string());
        if (child_op_type == "Top") {
          const auto top = child.child("Top");
          const auto actual_anti_child = top.child("RelOp");
          children.push_back(actual_anti_child);
          continue;
        }
      }
      children.push_back(child);
    }
    
    if (children.size() == 2) {
      auto inner_rel_op = children.front(); // Already reversed in nl_children
      
      if (inner_rel_op) {
        auto condition = findSeekCondition(inner_rel_op, ctx);
        if (condition.empty()) {
            // Check if NestedLoops itself has a predicate
            const auto predicate = nested_loops.child("Predicate");
            if (predicate) {
                const auto scalar_op = predicate.child("ScalarOperator");
                if (scalar_op) {
                    condition = scalarOperatorToExpression(scalar_op, ctx);
                }
            }
        }
        
        // If it's an INNER join and still no condition, it might be a CROSS JOIN
        if (condition.empty() && logical_join_type == Join::Type::INNER) {
             return std::make_unique<Join>(Join::Type::CROSS, Join::Strategy::LOOP, "");
        }
        
        return std::make_unique<Join>(logical_join_type, Join::Strategy::LOOP, condition);
      }
    }
    
    return std::make_unique<Join>(logical_join_type, Join::Strategy::LOOP, "");
  }
  return nullptr;
}

std::unique_ptr<Node> handleScan(const xml_node& node_xml, MssqlExplainCtx& ctx, const std::string& physical_op, const std::string& pushed_filter) {
  const auto output_columns = node_xml.child("OutputList");
  std::string table_name;
  std::string alias;
  for (auto column : output_columns.children()) {
    table_name = std::string(column.attribute("Table").as_string());
    break;
  }
  const auto index_scan = node_xml.child("IndexScan");
  const auto table_scan = node_xml.child("TableScan");

  auto getTableAndAlias = [&](const xml_node& scan_node) {
      const auto object = scan_node.child("Object");
      if (object) {
          if (table_name.empty()) {
              table_name = object.attribute("Table").as_string();
          }
          if (alias.empty()) {
              alias = object.attribute("Alias").as_string();
          }
      }
  };

  if (index_scan) getTableAndAlias(index_scan);
  if (table_scan) getTableAndAlias(table_scan);

  if (!alias.empty()) {
    ctx.aliases.insert(alias);
  }

  // SQL Server can use IndexScan for columnstore scans.
  // We prefer to show ColumnStore as a SCAN type.
  const std::string storage = index_scan ? index_scan.attribute("Storage").as_string() : (table_scan ? table_scan.attribute("Storage").as_string() : "");
  const bool is_column_store = (storage == "ColumnStore");

  Scan::Strategy strategy = (index_scan && !is_column_store) ? Scan::Strategy::SEEK : Scan::Strategy::SCAN;

  if (table_name.empty()) {
    const auto output_list = node_xml.child("OutputList");
    for (auto column : output_list.children()) {
      table_name = std::string(column.attribute("Table").as_string());
      break;
    }
  }

  if (table_name.empty()) {
    throw ExplainException("Table name not found for node: " + physical_op);
  }

  std::unique_ptr<Node> node;
  MssqlDialect dialect(ctx);
  if (isScanOp(physical_op)) {
    node = std::make_unique<Scan>(table_name, strategy, alias, &dialect);
  } else {
    // This should technically not be reached if handleSpool is used for all Spools,
    // but just in case some other scan-like node is actually a producer.
    node = std::make_unique<Materialise>(physical_op);
  }

  std::string filter;
  if (index_scan) {
    const auto predicate = index_scan.child("Predicate");
    filter = scalarOperatorToExpression(predicate.child("ScalarOperator"), ctx);
  }
  if (table_scan) {
    const auto predicate = table_scan.child("Predicate");
    filter = scalarOperatorToExpression(predicate.child("ScalarOperator"), ctx);
  }

  if (!pushed_filter.empty()) {
    if (filter.empty()) {
      filter = pushed_filter;
    } else {
      std::string clean_pushed = cleanFilter(pushed_filter, ctx);
      std::string clean_filter = cleanFilter(filter, ctx);
      // We check for presence of EACH OTHER to handle overlapping predicates more robustly
      if (clean_filter.find(clean_pushed) == std::string::npos && 
          clean_pushed.find(clean_filter) == std::string::npos) {
        filter = "(" + filter + ") AND (" + pushed_filter + ")";
      } else if (clean_filter.find(clean_pushed) != std::string::npos) {
        // filter already contains pushed_filter, keep filter
      } else {
        // pushed_filter contains filter, use pushed_filter
        filter = pushed_filter;
      }
    }
  }
  filter = cleanFilter(filter, ctx);
  filter = deduplicateFilters(filter, ctx);
  node->setFilter(filter, &dialect);
  return node;
}


std::unique_ptr<Node> handleComputeScalar(const xml_node& node_xml, const MssqlExplainCtx& ctx, std::vector<xml_node>& children) {
  const auto compute_scalar = node_xml.child("ComputeScalar");
  std::vector<Column> columns_computed;
  MssqlDialect dialect(ctx);

  for (auto defined_value : compute_scalar.child("DefinedValues")) {
    auto name = definedValueExpression(defined_value, ctx);
    columns_computed.push_back(Column(name, &dialect));
  }

  children.push_back(compute_scalar.child("RelOp"));
  return std::make_unique<Projection>(columns_computed);
}

std::pair<std::unique_ptr<Node>, std::vector<xml_node>> createNodeFromXML(const xml_node& node_xml,
                                                                          MssqlExplainCtx& ctx,
                                                                          std::string pushed_filter) {
  const auto physical_op = std::string(node_xml.attribute("PhysicalOp").as_string());
  const auto logical_op = std::string(node_xml.attribute("LogicalOp").as_string());

  std::unique_ptr<Node> node;
  std::vector<xml_node> children;
  if (logical_op == "Top" || physical_op == "Top") {
    node = handleTop(node_xml, children);
  } else if (logical_op.find("Spool") != std::string::npos || physical_op.find("Spool") != std::string::npos) {
    node = handleSpool(node_xml, physical_op, children);
  } else if (logical_op == "Filter") {
    std::string new_pushed_filter;
    node = handleFilter(node_xml, ctx, children, new_pushed_filter);
  } else if (logical_op == "Sort" || physical_op == "Sort") {
    node = handleSort(node_xml, ctx, children);
  } else if (logical_op == "Segment") {
    node = handleSegment(node_xml, ctx, children);
  } else if (logical_op == "Aggregate") {
    node = handleAggregate(node_xml, ctx, physical_op, children);
  } else if (logical_op.find("Join") != std::string::npos || physical_op == "Hash Match" || physical_op == "Adaptive Join" || physical_op == "Nested Loops") {
    node = handleJoin(node_xml, ctx, physical_op, logical_op, children);
  } else if (isScanOp(physical_op)) {
    node = handleScan(node_xml, ctx, physical_op, pushed_filter);
  } else if (logical_op == "Compute Scalar") {
    node = handleComputeScalar(node_xml, ctx, children);
  } else if (logical_op == "Parallelism" || physical_op == "Parallelism") {
    // SQL Server is not a distributed database, so we skip the Parallelism node
    // and process its child instead.
    const auto parallelism_node = node_xml.child("Parallelism");
    const auto child_rel_op = parallelism_node.child("RelOp");
    if (child_rel_op) {
        auto result = createNodeFromXML(child_rel_op, ctx, pushed_filter);
        // We want to keep the row counts from the Parallelism node if the child doesn't have them
        if (result.first->rows_actual <= 0) {
            parseRowCount(result.first.get(), node_xml);
        }
        return result;
    }
    throw ExplainException("Parallelism node without child RelOp");
  } else if (physical_op == "Lazy Spool" || physical_op == "Eager Spool" || physical_op == "Table Spool" || logical_op == "Row Count Spool") {
    node = handleSpool(node_xml, physical_op, children);
  } else {
    throw ExplainException("Unknown node type: " + logical_op + " (" + physical_op + ")");
  }

  parseRowCount(node.get(), node_xml);
  return std::pair{std::move(node), children};
}

std::unique_ptr<Node> buildExplainNode(const xml_node& node_xml, MssqlExplainCtx& ctx) {
  auto [node, children] = createNodeFromXML(node_xml, ctx);
  for (auto child : children) {
    node->addChild(buildExplainNode(child, ctx));
  }
  return std::move(node);
}


std::unique_ptr<Plan> buildExplainPlan(const std::string& explain_output) {
  xml_document doc;
  const auto result = doc.load_buffer(explain_output.c_str(), explain_output.size());
  if (!result) {
    throw ExplainException("Parsing of XML from SQL Server failed on error: " + std::string(result.description()));
  }

  /* High lever stats about the query*/

  /* Find the actual statement we want to explain. A single roundtrip may contain multiple statements */
  const auto statements = doc.child("ShowPlanXML").child("BatchSequence").child("Batch").child("Statements");
  xml_node statement_node;
  for (auto statement : statements.children()) {
    const auto statement_type = std::string(statement.attribute("StatementType").as_string());
    if (statement_type == "SELECT") {
      statement_node = statement;
      break;
    }
  }
  const auto plan_node = statement_node.child("QueryPlan");

  const auto query_stats = plan_node.child("QueryTimeStats");
  const double execution_time = query_stats.attribute("ElapsedTime").as_double();
  const auto first_operator = plan_node.child("RelOp");

  MssqlExplainCtx ctx;
  std::unique_ptr<Node> root_node = buildExplainNode(first_operator, ctx);
  hoistMaterialisations(root_node);
  fixRowCounts(root_node.get());

  auto plan = std::make_unique<Plan>(std::move(root_node));
  plan->planning_time = 0;
  plan->execution_time = execution_time;

  return plan;
}


std::unique_ptr<explain::Plan> Connection::explain(const std::string_view statement, std::optional<std::string_view> name) {
  const std::string artifact_name = name.has_value() ? std::string(*name) : std::to_string(std::hash<std::string_view>{}(statement));
  const auto cached_xml = getArtefact(artifact_name, "xml");
  if (cached_xml) {
    PLOGI << "Using cached execution plan artifact for: " << artifact_name;
    return buildExplainPlan(*cached_xml);
  }

  const std::string explain_string = fetchLivePlan(statement);
  storeArtefact(artifact_name, "xml", explain_string);
  return buildExplainPlan(explain_string);
}

std::string Connection::fetchLivePlan(const std::string_view statement) {
  /* NOTE: We have to do multiple roundtrips here due to a limitation in SQL Servers handling of batches wiuth SET statements*/
  execute("SET STATISTICS XML ON");
  const auto result = fetchAll(statement);
  result->drain();
  const auto plan = result->nextResult();
  if (!plan) {
    throw ExplainException("No execution plan returned from SQL Server");
  }
  std::string explain_string;
  for (auto& row : plan->rows()) {
    explain_string = row.asString(0);
    break;
  }
  // Ensure we drain ANY remaining result sets before we close the STATISTICS XML
  plan->drain();
  while (plan->nextResult()) {
    plan->drain();
  }

  execute("SET STATISTICS XML OFF");
  return explain_string;
}
} // namespace sql::mssql
