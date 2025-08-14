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

        std::string compactSymbolic() const override;;

        std::vector<Column> columns_distribute;
        Strategy strategy;
    };
} // namespace sql::explain
