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
            switch (strategy) {
                case Strategy::HASH: return "HASH DISTRIBUTE";
                case Strategy::BROADCAST: return "BROADCAST";
                case Strategy::ROUND_ROBIN: return "ROUND ROBIN";
                case Strategy::GATHER: return "GATHER";
            }
            return "DISTRIBUTION";
        }

        std::vector<Column> columns_distribute;
        Strategy strategy;
    };
} // namespace sql::explain
