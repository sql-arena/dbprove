#include "result.h"

#include "row.h"


namespace sql::utopia {
    size_t Result::rowCount() const {
        switch (data) {
            case UtopiaData::EMPTY:
                return 0;
            case UtopiaData::N10:
                return 10;
        }
        return 0;
    }

    size_t Result::columnCount() const {
        switch (data) {
            case UtopiaData::EMPTY:
                return 1;
            case UtopiaData::N10:
                return 11;
        }
        return 0;
    }

    std::unique_ptr<sql::RowBase> utopia::Result::nextRow() {
        switch (data) {
            case UtopiaData::EMPTY:
                return nullptr;
            case UtopiaData::N10:
                if (rowNumber > rowCount()) {
                    return nullptr;
                }

                SqlVariant v(rowNumber);
                ++rowNumber;
                return std::make_unique<Row>(std::vector({v}));
        }
        return nullptr;
    }
}
