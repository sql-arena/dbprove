#pragma once
#include "connection_base.h"
#include "engine.h"
#include "credential.h"
#include <atomic>


namespace sql {
class ConnectionBase;
/// @brief Factory class for creating connections using a specific engine
/// New driver implementors of engines must extend this factory class and the `Engine` enum
class ConnectionFactory {
  const Credential credential_;
  const Engine engine_;
  std::atomic<size_t> connectionCount_{0};

public:
  ConnectionFactory(const Engine& engine, const Credential& credential)
    : credential_(credential)
    , engine_(engine) {
  }
  ConnectionFactory(const ConnectionFactory& other)
  : credential_(other.credential_)
  , engine_(other.engine_)
  , connectionCount_(other.connectionCount_.load()) {
  }

  ConnectionFactory() = delete;

  std::unique_ptr<ConnectionBase> create();
};


}  // namespace sql