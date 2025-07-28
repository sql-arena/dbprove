#pragma once

#include <string>
#include <variant>

namespace sql {
class CredentialFile {
public:
  const std::string path;

  explicit CredentialFile(const std::string& path)
    : path(path) {
  }
};

class CredentialPassword {
public:
  const std::string host;
  const uint16_t port;
  const std::string database;
  const std::string username;
  const std::string password;

  CredentialPassword(const std::string& host,
                     const std::string& database,
                     const uint16_t port,
                     const std::string& username,
                     const std::string& password)
    : database(database)
    , host(host)
    , port(port)
    , username(username)
    , password(password) {
  }
};

class CredentialNone {
public:
  const std::string name;

  explicit CredentialNone(const std::string& name)
    : name(name) {
  }
  CredentialNone()
    : name("(unknown)") {
  }
};

using Credential = std::variant<CredentialFile, CredentialPassword, CredentialNone>;
;
}