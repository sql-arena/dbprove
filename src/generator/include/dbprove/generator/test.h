#pragma once
#include "dbprove/generator/generator_state.h"

namespace generator {
void pk_gen(GeneratorState&, sql::ConnectionBase*);
void fk_gen(GeneratorState&, sql::ConnectionBase*);
}

REGISTER_GENERATOR(
    "test.pk",
    "test",
    "CREATE TABLE test.pk (id INT PRIMARY KEY, val STRING)",
    generator::pk_gen,
    5
)

REGISTER_GENERATOR(
    "test.fk",
    "test",
    "CREATE TABLE test.fk (id INT PRIMARY KEY, pk_id INT, fk_val STRING)",
    generator::fk_gen,
    10
)
