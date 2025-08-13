#pragma once
#include <random>
#include <span>
#include <vector>

#include "generator_object.h"

namespace generator
{
    template <typename T>
    class WeightedSelect : GeneratorObject
    {
        std::vector<T> items_;
        std::vector<unsigned> weights_;
        std::discrete_distribution<size_t> distribution_;

    public:
        explicit WeightedSelect(const std::span<const std::pair<T, unsigned>> discreetDistribution)
        {
            for (auto [item, weight] : discreetDistribution) {
                items_.push_back(item);
                weights_.push_back(weight);
            }
            distribution_ = std::discrete_distribution<size_t>(weights_.begin(), weights_.end());
        }

        T next()
        {
            return items_[distribution_(gen_)];
        }
    };
}
