#pragma once
#include <string>
#include <utility>
#include <cstdint>
#include "credential_base.h"

namespace sql
{
    /// @brief Represents the credentials required to connect to a database.
    class CredentialPassword: public CredentialBase
    {
    public:
        const std::string host;
        const uint16_t port;
        const std::string username;
        const std::string password;

        CredentialPassword(const std::string& host,
                   const std::string& database,
                   const uint16_t port,
                   const std::string& username,
                   const std::string& password)
            : CredentialBase(database)
            , host(host)
            , port(port)
            , username(username)
            , password(password)
        {
        }
    };
}
