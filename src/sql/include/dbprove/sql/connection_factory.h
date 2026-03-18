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
  const std::optional<std::string> artifacts_path_;
  std::atomic<size_t> connectionCount_{0};

public:
  ConnectionFactory(const Engine& engine, const Credential& credential,
                    std::optional<std::string> artifacts_path = std::nullopt)
    : credential_(credential)
    , engine_(engine)
    , artifacts_path_(std::move(artifacts_path)) {
  }

  ConnectionFactory(const ConnectionFactory& other)
    : credential_(other.credential_)
    , engine_(other.engine_)
    , artifacts_path_(other.artifacts_path_)
    , connectionCount_(other.connectionCount_.load()) {
  }

  ConnectionFactory() : credential_(CredentialNone()), engine_(Engine::Type::Utopia) {}

  std::unique_ptr<ConnectionBase> create();
  const Engine& engine() const { return engine_; }
};
} // namespace sql
