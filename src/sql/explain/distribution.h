#pragma once
#include "explain/node.h"

namespace sql::explain
{
    class Column;

    class Distribute : public Node
    {
    public:
        enum class Strategy
        {
            HASH, BROADCAST, ROUND_ROBIN, GATHER
        };

        explicit Distribute(const Strategy strategy, const std::vector<Column>& columns_distribute = {})
            : Node(NodeType::DISTRIBUTE)
            , columns_distribute(columns_distribute)
            , strategy(strategy)
        {
        }

        [[nodiscard]] std::string compactSymbolic() const override;
        [[nodiscard]] std::string renderMuggle(size_t max_width) const override {
            std::string result;
            switch (strategy) {
                case Strategy::HASH: result = "DISTRIBUTE HASH"; break;
                case Strategy::BROADCAST: result = "DISTRIBUTE BROADCAST"; break;
                case Strategy::ROUND_ROBIN: result = "DISTRIBUTE ROUND ROBIN"; break;
                case Strategy::GATHER: result = "DISTRIBUTE GATHER"; break;
            }
            if (!columns_distribute.empty()) {
                result += " ON ";
                for (size_t i = 0; i < columns_distribute.size(); ++i) {
                    result += columns_distribute[i].name;
                    if (i + 1 < columns_distribute.size()) result += ", ";
                }
            }
            return result;
        }

        std::vector<Column> columns_distribute;
        Strategy strategy;
    };
} // namespace sql::explain
