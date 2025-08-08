#pragma once
#include <vector>
#include <nlohmann/json_fwd.hpp>

#include "result_base.h"

namespace sql::databricks
{
    class Row;

    class Result final : public ResultBase
    {
        friend class Row;
        std::vector<std::vector<SqlVariant>> rows_;
        std::vector<SqlTypeKind> columnTypes_;
        RowCount rowCount_ = 0;
        Row* currentRow_ ;
        RowCount currentRowIndex_ = 0;
        void parseRows(const nlohmann::json& result);
    public:
        explicit Result(const nlohmann::json& response);
        RowCount rowCount() const override;
        ColumnCount columnCount() const override;

    protected:
        const RowBase& nextRow() override;
    };
}
