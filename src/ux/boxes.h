#pragma once
#include "include/dbprove/ux/ux.h"
#include "glyphs.h"
#include <fort.hpp>
#include <ostream>
#include "colour.h"


namespace dbprove::ux {
using namespace fort;

enum class BorderStyle {
  SINGLE, DOUBLE
};


class Box {
public:
  explicit Box(Distance width, Distance height);
  explicit Box(Distance height);
  Box();

  Box& setTextColour(const Colour colour) {
    text_colour = colour;
    return *this;
  }

  Box& setBorderColour(const Colour colour) {
    border_colour = colour;
    return *this;
  }

  Box& setBorderStyle(const BorderStyle style) {
    border_style = style;
    return *this;
  }

  Box& setText(std::string_view text) {
    this->text = text;
    return *this;
  }

  void render(std::ostream& out) const;

  const Distance width;
  const Distance height;
  BorderStyle border_style = BorderStyle::SINGLE;
  Colour text_colour = Colour::WHITE;
  Colour border_colour = Colour::BLACK;
  std::string text;
};
}