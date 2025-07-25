#pragma once

#include <stdexcept>
#include <string>

#include "credential_base.h"

namespace sql {
/**
 * @brief Base exception class for all SQL-related errors
 * @note: In general, already throw things that inherit from SqlException inside
 * connector code
 */
class Exception : public std::runtime_error {
public:
  explicit Exception(const std::string& message)
    : std::runtime_error(message) {
  }

  explicit Exception(std::string_view message)
    : std::runtime_error(std::string(message)) {
  }
};

/**
 * @brief Exception thrown when a connection to the database cannot be established
 */
class ConnectionException final : public Exception {
public:
  explicit ConnectionException(
      const CredentialBase& credential,
      const std::string& message)
    : Exception("When trying to access" + credential.database + " the connector threw: " + message) {
  }
};

/**
 * @brief Thrown when a SQL statement is malformed
 */
class SyntaxException final : public Exception {
public:
  explicit SyntaxException(const std::string& message)
    : Exception("SQL syntax error: " + message) {
  }
};

/**
 * @brief Exception thrown when a statement times out
 */
class StatementTimeoutException final : public Exception {
public:
  explicit StatementTimeoutException(const std::string& message = "Query execution timed out")
    : Exception(message) {
  }
};

/**
 * @brief Exception thrown when a connection times out
 */
class ConnectionTimeoutException final : public Exception {
public:
  explicit ConnectionTimeoutException(const std::string& message = "Query execution timed out")
    : Exception(message) {
  }
};
}
