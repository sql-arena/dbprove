#pragma once
#include <duckdb/main/query_result.hpp>


namespace sql::duckdb {
struct ResultHolder {
  std::unique_ptr<::duckdb::QueryResult> result;
};
}