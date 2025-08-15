#include "theorem.h"
#include <string>

namespace dbprove::theorem {
std::string DataExplain::render() {
  return plan->render();
}

}