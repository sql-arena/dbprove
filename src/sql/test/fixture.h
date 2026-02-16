#pragma once
#include <string_view>
#include <vector>
#include <dbprove/sql/sql.h>

#include "dbprove/sql/connection_factory.h"

/**
 * For testing actual connectivity, we rely on there being standard configuration, currently assumed to be on
 * localhost  Since we don't care about security for this use case
 * just construct connection with hardcoded password that are set up as part of the test case
 * TODO: Change the host location to be a parameter as we will eventually want to have these things be remote.
 * @return Factories usable for testing
 */
std::vector<sql::ConnectionFactory> factories(const bool local_only = true, const std::string_view find = "");
