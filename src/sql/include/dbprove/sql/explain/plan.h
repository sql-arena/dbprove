#pragma once
#include "node.h"
#include <memory>
#include <ostream>
#include <magic_enum/magic_enum.hpp>


namespace sql::explain
{
    /**
     * Represents the high level operations that roughly map to node types
     */
    enum class Operation
    {
        JOIN = 0, AGGREGATE = 1, SORT = 2, SCAN = 3, FILTER = 4,
    };

    inline std::string_view to_string(const Operation op) { return magic_enum::enum_name(op); }

    class Node;

    struct Magnitude
    {
        int8_t value;
        std::string to_string() const;

        bool operator<(const Magnitude& other) const
        {
            return value < other.value;
        }
    };

    class Plan
    {
        std::unique_ptr<Node> plan_tree;

    public:
        struct MisEstimation
        {
            static constexpr int8_t INFINITE_OVER = 4;
            static constexpr int8_t INFINITE_UNDER = -4;
            Operation operation;
            Magnitude magnitude;
            size_t count;

            bool operator<(const MisEstimation& other) const
            {
                if (magnitude.value != other.magnitude.value) {
                    return magnitude.value < other.magnitude.value;
                }
                return operation < other.operation;
            }
        };

        enum class RenderMode
        {
            SYMBOLIC,
            MUGGLE
        };

        explicit Plan(std::unique_ptr<Node> root_node)
            : plan_tree(std::move(root_node))
        {
        }

        double planning_time = 0.0;
        double execution_time = 0.0;

        [[nodiscard]] Node& planTree() const
        {
            return *plan_tree;
        }

        [[nodiscard]] RowCount rowsAggregated() const;
        [[nodiscard]] RowCount rowsSorted() const;
        [[nodiscard]] RowCount rowsJoined() const;
        [[nodiscard]] RowCount rowsProcessed() const;
        [[nodiscard]] RowCount rowsReturned() const;
        [[nodiscard]] RowCount rowsScanned() const;
        [[nodiscard]] RowCount rowsFiltered() const;
        [[nodiscard]] std::vector<MisEstimation> misEstimations() const;

        /**
         * Render the plan to human-readable format
         * @param mode Mode to use for rendering
         */
        void render(std::ostream& out, size_t max_width, RenderMode mode = RenderMode::MUGGLE) const;;
    };
}
