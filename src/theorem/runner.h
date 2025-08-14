#pragma once
#include <vector>

#include "types.h"
#include "../sql/include/dbprove/sql/connection_factory.h"
#include "../sql/include/dbprove/sql/query.h"


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
  void serial(const std::span<Query>& queries, size_t iterations = 0) const;

  /**
   * @brief As above, but for single query
   * @param query to run
   * @param iterations to run off query
   */
  void serial(Query& query, size_t iterations = 0) const;
  /**
   * @brief Run the same queries on all threads
   * @param threadCount Number of threads to execute
   * @param queries Queries to execute on all threads
  */
  void parallelApart(size_t threadCount, std::span<Query>& queries) const;

  /**
   * @brief Run queries across threads as fast as possible
   * @param threadCount Number of threads to execute
   * @param queries Queries to execute. Each thread picks up from the same pool
  */

  void parallelTogether(size_t threadCount, std::span<Query>& queries) const;

  /**
   * Explain queries and add to proof data
   * @param queries To run
   * @param state To update
   */
  void serialExplain(std::span<Query>& queries, TheoremProof& state) const ;

  void serialExplain(Query&& query, TheoremProof& state)  const {
    std::vector<Query> queries;
    queries.push_back(std::move(query));
    auto span = std::span(queries);
    serialExplain(span, state);
  };

};