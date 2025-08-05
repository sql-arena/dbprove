#pragma once

#include <string>
#include <variant>

namespace sql {
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
  const std::string password;

  CredentialPassword(std::string host,
                     std::string database,
                     const uint16_t port,
                     std::string username,
                     std::string password)
    : database(std::move(database))
    , host(std::move(host))
    , port(port)
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
  explicit CredentialAccessToken(std::string token)
    : token(std::move(token)) {
  }

  const std::string token;
};


using Credential = std::variant<CredentialFile,
                                CredentialPassword,
                                CredentialNone,
                                CredentialAccessToken>;
;
}