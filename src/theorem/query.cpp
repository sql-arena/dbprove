#include "query.h"

namespace dbprove::theorem {
thread_local std::vector<QueryStats> Query::thread_stats_;
}