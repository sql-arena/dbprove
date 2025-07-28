#pragma once
#include "connection_base.h"
#include "Engine.h"
#include "utopia/connection.h"
#include "msodbc/connection.h"
#include "credential.h"
#include <atomic>
#include <cassert>

#include "postgres/connection.h"

namespace sql {
/// @brief Factory class for creating connections using a specific engine
/// New driver implementors of engines must extend this factory class and the `Engine` enum
class ConnectionFactory {
  const Credential credential_;
  const Engine engine_;
  std::atomic<size_t> connectionCount_{0};

public:
  ConnectionFactory(Engine engine, const Credential& credential)
    : credential_(credential)
    , engine_(engine) {
  }
  ConnectionFactory(const ConnectionFactory& other)
  : credential_(other.credential_)
  , engine_(other.engine_)
  , connectionCount_(other.connectionCount_.load()) {
  }

  std::unique_ptr<ConnectionBase> create() {
    connectionCount_.fetch_add(1);
    switch (engine_.type()) {
      case Engine::Type::Utopia:
        return std::make_unique<utopia::Connection>();
      case Engine::Type::Postgres:
        if (!std::holds_alternative<CredentialPassword>(credential_)) {
          throw std::invalid_argument("Postgres engine requires a password credential");
        }
        return std::make_unique<postgres::Connection>(std::get<CredentialPassword>(credential_));
      case Engine::Type::SQLServer:
        return std::make_unique<msodbc::Connection>(credential_);
      default:
        throw std::invalid_argument("Unsupported engine type: " + engine_.name());
    }
  }



};
}  // namespace sql