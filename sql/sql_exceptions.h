#pragma once

#include <stdexcept>
#include <string>

#include "Credential.h"

namespace sql {
/**
 * @brief Base exception class for all SQL-related errors
 * @note: In general, already throw things that inherit from SqlException inside
 * connector code
 */

class SqlException : public std::runtime_error {
public:
  explicit SqlException(const std::string& message)
      : std::runtime_error(message) {}

  explicit SqlException(std::string_view message)
      : std::runtime_error(std::string(message)) {}
};

/**
 * @brief Exception thrown when a connection to the database cannot be established
 */
class ConnectionException : public SqlException {
public:
  explicit ConnectionException(
    const Credential& credential,
    const std::string& message)
      : SqlException("When trying to access" + credential.host + " the connector threw: "  + message) {}
};

class SyntaxException : public SqlException {
public:
  explicit SyntaxException(const std::string& message)
      : SqlException("SQL syntax error: " + message) {}
};

/**
 * @brief Exception thrown when a query times out
 */
class TimeoutException : public SqlException {
public:
  explicit TimeoutException(const std::string& message = "Query execution timed out")
      : SqlException(message) {}
};



}