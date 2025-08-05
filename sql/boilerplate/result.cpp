#include "result.h"

namespace sql::boilerplate {


  class Result::Pimpl {
    void* handle_;
    public:
      Pimpl(void* handle): handle_(handle) {}
  };

  Result::Result(void* handle)
    : impl_(std::make_unique<Pimpl>(handle)) {
  }
}