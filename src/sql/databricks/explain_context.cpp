#include "explain_context.h"
#include <plog/Log.h>

namespace sql::databricks {

void ExplainContext::dump() const {
    PLOGD << "Explain Context Dump:";
    PLOGD << "  Row Estimates:";
    for (const auto& [node, estimate] : row_estimates) {
        PLOGD << "    " << node << ": " << estimate;
    }
    
    if (!raw_explain.empty()) {
        PLOGD << "  Raw EXPLAIN COST (first 100 chars): " << raw_explain.substr(0, 100) << "...";
    }
    
    if (!scraped_plan.empty()) {
        PLOGD << "  Scraped JSON (pretty printed): " << scraped_plan.dump(4);
    }
}

} // namespace sql::databricks
