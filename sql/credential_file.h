#pragma once
#include "credential_base.h"

namespace sql {
class CredentialFile : public CredentialBase {
  std::string path;

public:
  explicit CredentialFile(const std::string& path)
    : CredentialBase(path)
    , path(path) {
  }
};
}