#pragma once

#include <dbprove/common/string.h>

#include <map>
#include <stdexcept>

namespace sql
{
    /// @brief Which database engine to use
    class Engine
    {
    public:
        enum class Type
        {
            MariaDB,
            Postgres,
            SQLite,
            SQLServer,
            Oracle,
            DuckDB,
            Utopia,
            Databricks,
            ClickHouse,
            Yellowbrick
        };

        explicit Engine(const Type type): type_(type) {
        }
        explicit Engine(const std::string_view name)
        {
            const std::string name_lower = to_lower(name);
            if (!known_names.contains(name_lower)) {
                throw std::runtime_error("Engine '" + std::string(name) + "' not found");
            }
            type_ = known_names[name_lower];
        }

        /**
         * @brief When no endpoint or database connectivity is supplied, this is the default
         */
        std::string defaultHost(std::optional<std::string> host = std::nullopt) const;

        /**
         * @brief Default port (or 0 if no port needed) if not provided
         */
        uint16_t defaultPort(uint16_t port) const;

        /**
         * @brief Default database if none supplied
         */
        std::string defaultDatabase(std::optional<std::string> database = std::nullopt) const;
        /**
         * @brief If username is not supplied for this engine, this is the default.
         */
        std::string defaultUsername(std::optional<std::string> username = std::nullopt) const;
        /**
         * @brief If token is not supplied for this engine, this is the default.
         */
        std::string defaultToken(std::optional<std::string> token = std::nullopt) const;
        /// @brief  Provide entries here for friendly names of engines. These must be lowercase as we string match them
        inline static std::map<std::string_view, Type> known_names = {
            {"mariadb", Type::MariaDB},
            {"mysql", Type::MariaDB},
            {"postgresql", Type::Postgres},
            {"postgres", Type::Postgres},
            {"azurefabricwarehouse", Type::SQLServer},
            {"pg", Type::Postgres},
            {"sqlite", Type::SQLite},
            {"sqlserver", Type::SQLServer},
            {"duckdb", Type::DuckDB},
            {"duck", Type::DuckDB},
            {"utopia", Type::Utopia},
            {"databricks", Type::Databricks},
            {"utopia", Type::Utopia}
        };

        [[nodiscard]] std::string name() const
        {
            return std::string(canonical_names.at(type_));
        }

        [[nodiscard]] Type type() const { return type_; }

    private:
        Type type_;
        /// @brief The name by which the engine is officially known
        inline static std::map<Type, std::string_view> canonical_names = {
            {Type::MariaDB, "MySQL"},
            {Type::Postgres, "PostgreSQL"},
            {Type::SQLite, "SQLite"},
            {Type::Utopia, "Utopia"},
            {Type::DuckDB, "DuckDB"},
            {Type::Databricks, "Databricks"}
        };
    };
}
