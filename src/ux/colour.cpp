#include "colour.h"
#include <fort.hpp>
#include <map>

namespace dbprove::ux {
const static std::map<Colour, fort::color> map = {
    {Colour::RED, fort::color::red},
    {Colour::BLUE, fort::color::blue},
    {Colour::WHITE, fort::color::default_color},
    {Colour::BLACK, fort::color::black},
    {Colour::GREY, fort::color::light_gray}
};

/**
 * Map from our own colour palette to the fort specific enums
 * @note Forts light_whyte [sic] does not actually work and causes a segfault, so we map to default
 * @param colour
 * @return
 */
fort::color mapFortColour(const Colour colour) {
  if (!map.contains(colour)) {
    throw std::runtime_error("Unknown colour");
  }
  return map.at(colour);
}
}