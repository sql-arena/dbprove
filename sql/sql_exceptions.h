#pragma once

#include <stdexcept>
#include <string>

#include "credential.h"

namespace sql {
/**
 * @brief Base exception class for all SQL-related errors
 * @note: In general, already throw things that inherit from SqlException inside
 * connector code
 */
class Exception : public std::runtime_error {
  static constexpr const char* kDatabaseError = "Database error";

public:
  const std::string statement = "";
  const std::string full_message = "";

  explicit Exception(const std::string& message)
    : std::runtime_error(kDatabaseError)
    , full_message(std::string(message)) {
  }

  explicit Exception(const std::string& message, const std::string_view statement)
    : std::runtime_error(kDatabaseError)
    , statement(statement) {
  }

  explicit Exception(std::string_view message)
    : std::runtime_error(kDatabaseError)
    , full_message(std::string(message)) {
  }

  const char* what() const noexcept override {
    return full_message.c_str();
  }
};

/// @brief userfriendly message from credential info
static constexpr std::string render_credential(const Credential& credential) {
  if (std::holds_alternative<CredentialPassword>(credential)) {
    const auto cred_password = std::get<CredentialPassword>(credential);
    return "database: " + cred_password.database + " as user " + cred_password.username;
  }
  if (std::holds_alternative<CredentialFile>(credential)) {
    const auto cred_file = std::get<CredentialFile>(credential);
    return "file path: " + cred_file.path;
  }
  return "Unknown credential";
}

/**
 * @brief Exception thrown when a connection to the database cannot be established
 */
class ConnectionException final : public Exception {
public:
  explicit ConnectionException(
      const Credential& credential,
      const std::string& message)
    : Exception("When trying to access" + render_credential(credential) + " the connector threw:\n" + message) {
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

/**
 * @brief Thrown when the user expected to get data, but got nothing
 */
class EmptyResultException final : public Exception {
public:
  explicit EmptyResultException(const std::string_view query)
    : Exception("Expected to get data from query: " + std::string(query) + ", but got nothing") {
  }
};

class InvalidTypeException final : public Exception {
public:
  explicit InvalidTypeException(const std::string& error)
    : Exception("Did not recognise type with name: " + error) {
  }
};


class InvalidPlanException final : public Exception {
public:
  explicit InvalidPlanException(const std::string& error)
    : Exception(error) {
  }
};


class InvalidColumnsException final : public Exception {
public:
  explicit InvalidColumnsException(const std::string& error, const std::string_view statement)
    : Exception(error, statement) {
  }
  explicit InvalidColumnsException(const std::string& error)
    : Exception(error) {
  }
};

class InvalidRowsException final : public Exception {
public:
  explicit InvalidRowsException(const std::string& error, const std::string_view statement)
    : Exception(error, statement) {
  }
};
}