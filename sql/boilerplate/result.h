#pragma once
#include "result_base.h"

namespace sql::boilerplate {
  class Result: public ResultBase {
    class Pimpl;
    std::unique_ptr<Pimpl> impl_;
    public:
    explicit Result(void* handle);
  };
}