#pragma once
#include "../postgresql/connection.h"


namespace sql::yellowbrick
{
    class Connection final : public postgresql::Connection
    {
    public:
        Connection(const CredentialPassword& credential, const Engine& engine);
        std::string version() override;
        std::string translateDialectDdl(const std::string_view ddl) const override;
        std::unique_ptr<explain::Plan> explain(std::string_view statement) override;
        void analyse(std::string_view table_name) override;
    };
}
