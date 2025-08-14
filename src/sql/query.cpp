#include "include/dbprove/sql/query.h"

thread_local std::vector<QueryStats> Query::thread_stats_;