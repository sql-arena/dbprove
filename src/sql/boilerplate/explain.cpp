#include "connection.h"

namespace sql::boilerplate {
std::unique_ptr<explain::Plan> Connection::explain(std::string_view statement) {
  // TODO: parse explain and turn into canonical format as per the `sql/explain` directory
  return nullptr;
}
}