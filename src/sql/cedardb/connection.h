#pragma once
#include "postgresql/connection.h"

namespace sql::cedardb {
/**
 * CedarDB is wire-protocol compatible with PostgreSQL (libpq), so we inherit
 * all connection, result, and bulk-load logic from the PostgreSQL driver.
 * Only EXPLAIN and version() need their own implementations.
 */
class Connection : public postgresql::Connection {
public:
  using postgresql::Connection::Connection;

  std::unique_ptr<explain::Plan> explain(std::string_view statement,
                                         std::optional<std::string_view> name = std::nullopt) override;
  std::string version() override;
};
} // namespace sql::cedardb
