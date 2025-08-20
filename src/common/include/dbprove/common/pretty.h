#pragma once
#include <string>

namespace dbprove::common
{
    /**
     * Render an always TARGET_SIZE glyph long representation of a count value
     * @param count
     * @return
     */
    std::string PrettyHumanCount(size_t count);
}
