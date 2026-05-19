#pragma once
#include <duckdb.hpp>


namespace sql::duckdb {
struct ResultHolder {
  std::unique_ptr<::duckdb::QueryResult> result;
};
}
