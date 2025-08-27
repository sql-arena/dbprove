#pragma once
#include "../postgres/row.h"


namespace sql::yellowbrick
{
    class Row final : public postgres::Row
    {
    public:
        Row(PGresult* result, int row_number)
            : postgres::Row(result, row_number)
        {
        }

        explicit Row(PGresult* result)
            : postgres::Row(result)
        {
        }

        explicit Row(const postgres::Row& other)
            : postgres::Row(other)
        {
        }
    };
}
