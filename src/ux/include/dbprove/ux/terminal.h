#pragma once
#include <cstdint>

namespace dbprove::ux
{
    class Terminal
    {
    public:
        static inline uint16_t SCREEN_WIDTH = 120;

        static void configure();
    };
}
