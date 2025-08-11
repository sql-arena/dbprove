#include "credential.h"

#include <array>
#include <stdexcept>
#include "common/config.h"

namespace sql {
CredentialAccessToken::CredentialAccessToken(Engine engine, std::string endpoint_url, std::string database,
                                             std::string token)
  : token(std::move(token))
  , database(std::move(database))
  , endpoint_url(std::move(endpoint_url))
  , engine(engine) {
}

CredentialAccessToken::CredentialAccessToken(const Engine engine)
  : token(engine.defaultToken())
  , endpoint_url(engine.defaultHost())
  , database(engine.defaultDatabase())
  , engine(engine) {
}
}