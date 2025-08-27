#pragma once
#include "../postgres/result.h"

namespace sql::yellowbrick
{
  class Result final : public postgres::Result
  {
  public:
    explicit Result(PGresult* data);
  };
}
