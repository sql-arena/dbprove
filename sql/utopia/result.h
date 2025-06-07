#pragma once
#include <result_base.h>
#include <cstddef>
#include "row_base.h"

namespace utopia {

    enum class UtopiaData {
        EMPTY,
        N10,
        NOOP
    };

    struct UtopiaConfig { utopia::
        UtopiaData data;
        uint32_t runtime_us;
    };


    class Result : public sql::ResultBase {
    public:
        explicit Result(const UtopiaData data = UtopiaData::EMPTY): data(data) {};

        ~Result() override {
        };

        size_t rowCount() const override;
        size_t columnCount() const override;

    protected:
        void reset() override {
        };

        std::unique_ptr<sql::RowBase> nextRow() override;

    private:
        const UtopiaData data;
        size_t rowNumber = 0;
    };
}
