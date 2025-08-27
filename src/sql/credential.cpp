#include "include/dbprove/sql/credential.h"
#include "include/dbprove/sql/engine.h"

namespace sql {
CredentialAccessToken::CredentialAccessToken(const Engine& engine, std::string endpoint_url, std::string database,
                                             std::string token)
  : token(std::move(token))
  , database(std::move(database))
  , endpoint_url(std::move(endpoint_url)) {
}

CredentialAccessToken::CredentialAccessToken(const Engine& engine)
  : token(engine.defaultToken())
  , endpoint_url(engine.defaultHost())
  , database(engine.defaultDatabase()) {
}
}