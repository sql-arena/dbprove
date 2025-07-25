#pragma once
#include <string>
#include <utility>
#include <vector>

namespace sql {
class CredentialBase {
public:
  std::vector<std::pair<std::string, std::string>> options;
  const std::string database;

  explicit CredentialBase(const std::string& database)
    : database(database) {
  }
  virtual ~CredentialBase() = default;

};
}