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

/**
 * The ANSI SQL standard prescribes a series of error codes that database should adopt.
 *
 * Postgres generally does a good job at this, but others are lacking. Hence, as part of our
 * error handling, we map to a common set of SqlState errors so the caller no longer has to
 * worry about the intricacies of individual databases.
 */
enum class SqlState {
  SUCCESS_00,
  WARNING_01,
  NO_DATA_02,
  CONNECTION_08,
  DATA_EXCEPTION_22,
  INVALID_CURSOR_24,
  INVALID_AUTHORIZATION_SPECIFICATION_28,
  NOT_IMPLEMENTED_0A,
  TRANSACTION_ERROR_40,
  SYNTAX_ERROR_42,
  INVALID_TABLE_42P01,
  PRODUCT_ERROR_56,
  RESOURCE_UNAVAILABLE_57,
};

class Exception : public std::runtime_error {
  static constexpr auto kDatabaseError = "Database error";

public:
  const std::string statement = "";
  const std::string full_message = "";
  const SqlState sql_state_class;

  explicit Exception(const SqlState sql_state_class, const std::string& message)
    : std::runtime_error(kDatabaseError)
    , full_message(std::string(message))
    , sql_state_class(sql_state_class) {
  }

  explicit Exception(const SqlState sql_state_class, const std::string& message, const std::string_view statement)
    : std::runtime_error(kDatabaseError)
    , statement(statement)
    , full_message(std::string(statement) + "\n" + message)
    , sql_state_class(sql_state_class) {
  }

  explicit Exception(const SqlState sql_state_class, const std::string_view message)
    : std::runtime_error(kDatabaseError)
    , full_message(std::string(message))
    , sql_state_class(sql_state_class) {
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
    : Exception(SqlState::CONNECTION_08,
                "When trying to access: " + render_credential(credential) + " the connector threw:\n" + message) {
  }
};

/**
 * @brief Thrown when a SQL statement is malformed
 */
class SyntaxException final : public Exception {
public:
  explicit SyntaxException(const std::string& message)
    : Exception(SqlState::SYNTAX_ERROR_42, "SQL syntax error: " + message) {
  }

  explicit SyntaxException(const std::string& message, const std::string_view statement)
    : Exception(SqlState::SYNTAX_ERROR_42, "SQL syntax error: " + message, statement) {
  }
};

/**
 * @brief Exception thrown when a statement times out
 */
class StatementTimeoutException final : public Exception {
public:
  explicit StatementTimeoutException(const std::string& message = "Query execution timed out")
    : Exception(SqlState::RESOURCE_UNAVAILABLE_57, message) {
  }
};

/**
 * @brief Exception thrown when a connection times out
 */
class ConnectionTimeoutException final : public Exception {
public:
  explicit ConnectionTimeoutException(const std::string& message = "Query execution timed out")
    : Exception(SqlState::CONNECTION_08, message) {
  }
};

/**
 * @brief Thrown when the user expected to get data, but got nothing
 */
class EmptyResultException final : public Exception {
public:
  explicit EmptyResultException(const std::string_view query)
    : Exception(SqlState::NO_DATA_02, "Expected to get data from query: " + std::string(query) + ", but got nothing") {
  }
};

class InvalidTypeException final : public Exception {
public:
  explicit InvalidTypeException(const std::string& error)
    : Exception(SqlState::DATA_EXCEPTION_22, "Did not recognise type with name: " + error) {
  }
};

class InvalidTableException final : public Exception {
public:
  explicit InvalidTableException(const std::string& error)
    : Exception(SqlState::INVALID_TABLE_42P01, error) {
  }
};


class InvalidPlanException final : public Exception {
public:
  explicit InvalidPlanException(const std::string& error)
    : Exception(SqlState::PRODUCT_ERROR_56, error) {
  }

  explicit InvalidPlanException(const std::string& error, const std::string& statement)
    : Exception(SqlState::PRODUCT_ERROR_56, error, statement) {
  }
};


class PermissionDeniedException final : public Exception {
public:
  explicit PermissionDeniedException(const std::string& error)
    : Exception(SqlState::INVALID_AUTHORIZATION_SPECIFICATION_28, error) {
  }
};

class InvalidColumnsException final : public Exception {
public:
  explicit InvalidColumnsException(const std::string& error, const std::string_view statement)
    : Exception(SqlState::INVALID_CURSOR_24, error, statement) {
  }

  explicit InvalidColumnsException(const std::string& error)
    : Exception(SqlState::INVALID_CURSOR_24, error) {
  }
};

class InvalidRowsException final : public Exception {
public:
  explicit InvalidRowsException(const std::string& error, const std::string_view statement)
    : Exception(SqlState::INVALID_CURSOR_24, error, statement) {
  }
};


class TransactionException final : public Exception {
public:
  explicit TransactionException(const std::string& error)
    : Exception(SqlState::TRANSACTION_ERROR_40, error) {
  }
};


class ProtocolException final : public Exception {
public:
  explicit ProtocolException(const std::string& error)
    : Exception(SqlState::CONNECTION_08, error) {
  }
};

class ExplainException final : public Exception {
public:
  explicit ExplainException(const std::string& error)
    : Exception(SqlState::SYNTAX_ERROR_42, error) {
  }
};
}