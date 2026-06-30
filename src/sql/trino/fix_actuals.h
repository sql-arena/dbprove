#pragma once

#include "explain/plan.h"
#include "connection_base.h"
#include <string_view>

namespace sql::trino {

/**
 * Enrich a pre-built Trino plan tree with actual and estimated row counts by
 * running EXPLAIN ANALYZE on the original statement and parsing the text output.
 *
 * Unlike the generic Plan::fixActuals (which runs COUNT(*) per node), this
 * implementation executes the query exactly once via EXPLAIN ANALYZE and maps
 * the resulting per-node statistics back onto the plan tree.
 *
 * Distribute nodes receive their counts from their first child (bottom-up pass).
 */
void fixActualsFromExplainAnalyze(explain::Plan& plan,
                                   std::string_view statement,
                                   ConnectionBase& connection);

}  // namespace sql::trino
