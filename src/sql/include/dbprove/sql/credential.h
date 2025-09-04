#pragma once
#include <optional>
#include <string>
#include <variant>
#include <cstdint>

namespace sql {
class Engine;

class CredentialFile {
public:
  const std::string path;

  explicit CredentialFile(std::string path)
    : path(std::move(path)) {
  }
};

class CredentialPassword {
public:
  const std::string host;
  const uint16_t port;
  const std::string database;
  const std::string username;
  const std::optional<std::string> password;

  CredentialPassword(std::string host,
                     std::string database,
                     const uint16_t port,
                     std::string username,
                     std::optional<std::string> password)
    : host(std::move(host))
    , port(port)
    , database(std::move(database))
    , username(std::move(username))
    , password(std::move(password)) {
  }
};

/**
 * Access without asking for permissions (ex: DuckDb or Trino)
 */
class CredentialNone {
public:
  const std::string name;

  explicit CredentialNone(std::string name)
    : name(std::move(name)) {
  }

  CredentialNone()
    : name("(unknown)") {
  }
};

/**
 * Access token credentials typically passed via HTTP auth headers
 */
class CredentialAccessToken {
public:
  explicit CredentialAccessToken(const Engine& engine, std::string endpoint_url, std::string database,
                                 std::string token);
  explicit CredentialAccessToken(const Engine& engine);
  const std::string token;
  const std::string database;
  const std::string endpoint_url;
};


using Credential = std::variant<CredentialFile,
                                CredentialPassword,
                                CredentialNone,
                                CredentialAccessToken>;
;
}