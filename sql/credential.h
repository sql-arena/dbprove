#pragma once
#include <string>
#include <utility>
#include <cstdint>

namespace sql
{
    /// @brief Represents the credentials required to connect to a database.
    class Credential
    {
    public:
        const std::string host;
        const std::string database;
        const uint16_t port;
        const std::string username;
        const std::string password;
        const std::string database_;

        Credential(std::string host,
                   std::string database,
                   const uint16_t port,
                   std::string username,
                   std::string password)
            : host(std::move(host))
            , database(std::move(database))
            , port(port)
            , username(std::move(username))
            , password(std::move(password))
            , database_(database)
        {
        }
    };
}
