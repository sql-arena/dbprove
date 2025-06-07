#pragma once
#include <vector>
#include "sql/connection_factory.h"
#include "sql/query.h"


class Runner {
  sql::ConnectionFactory& factory_;

public:
  explicit Runner(sql::ConnectionFactory& factory)
    : factory_(factory) {
  }

  /**
   * @brief Serially run the queries
   * @param queries queries to run
   */
  void Serial(const std::span<Query>& queries, size_t iterations = 0) const;

  /**
   * @brief As above, but for single query
   * @param query to run
   * @param iterations to run off query
   */
  void Serial(Query& query, size_t iterations = 0) const;
  /**
   * @brief Run the same queries on all threads
   * @param threadCount Number of threads to execute
   * @param queries Queries to execute on all threads
  */
  void ParallelApart(size_t threadCount, std::span<Query>& queries) const;

  /**
   * @brief Run queries across threads as fast as possible
   * @param threadCount Number of threads to execute
   * @param queries Queries to execute. Each thread picks up from the same pool
  */

  void ParallelTogether(size_t threadCount, std::span<Query>& queries) const;
};