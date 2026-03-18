#include <dbprove/generator/generator_state.h>
#include <dbprove/sql/sql.h>
#include <plog/Log.h>
#include <fstream>

namespace generator {

void pk_gen(GeneratorState& state, sql::ConnectionBase*) {
    PLOGI << "Generating test.pk CSV input";
    const auto file_name = state.csvPath("test.pk");
    std::ofstream out(file_name);
    constexpr auto col_separator = GeneratorState::columnSeparator();
    constexpr auto row_separator = GeneratorState::rowSeparator();

    out << "id" << col_separator << "val" << row_separator;
    for (int i = 1; i <= 5; ++i) {
        out << i << col_separator << "val_" << i << row_separator;
    }
    state.registerGeneration("test.pk", file_name);
}

void fk_gen(GeneratorState& state, sql::ConnectionBase*) {
    PLOGI << "Generating test.fk CSV input";
    const auto file_name = state.csvPath("test.fk");
    std::ofstream out(file_name);
    constexpr auto col_separator = GeneratorState::columnSeparator();
    constexpr auto row_separator = GeneratorState::rowSeparator();

    out << "id" << col_separator << "pk_id" << col_separator << "fk_val" << row_separator;
    for (int i = 1; i <= 10; ++i) {
        const int pk_id = (i + 1) / 2;
        out << i << col_separator << pk_id << col_separator << "fk_val_" << i << row_separator;
    }
    state.registerGeneration("test.fk", file_name);
}

} // namespace generator
