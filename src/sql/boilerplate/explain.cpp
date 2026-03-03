#include "connection.h"

namespace sql::boilerplate {
std::unique_ptr<explain::Plan> Connection::explain(const std::string_view statement, std::optional<std::string_view> name) {
  // TODO: parse explain and turn into canonical format as per the `sql/explain` directory
  return nullptr;
}
}