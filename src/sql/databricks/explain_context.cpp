#include "explain_context.h"
#include <plog/Log.h>

namespace sql::databricks {

void ExplainContext::dump() const {
    PLOGD << "Explain Context Dump:";
    PLOGD << "  Row Estimates:";
    for (const auto& [node, estimate] : row_estimates) {
        PLOGD << "    " << node << ": " << estimate;
    }
}

} // namespace sql::databricks
