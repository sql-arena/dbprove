#pragma once
#include <string>
#include <string_view>
#include <ostream>

namespace dbprove::ux {
void PreAmple(std::ostream& out, const std::string_view title);

void PreAmpleTheorem(std::ostream& out, std::string_view theorem);
}
