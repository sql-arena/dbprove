#pragma once
#include "dbprove/generator/generator_state.h"

namespace generator {
void pk_gen(GeneratorState&, sql::ConnectionBase*);
void fk_gen(GeneratorState&, sql::ConnectionBase*);
}

REGISTER_GENERATOR(
    "test.pk",
    "CREATE TABLE test.pk (id INT PRIMARY KEY, val STRING)",
    generator::pk_gen,
    5
)

REGISTER_GENERATOR(
    "test.fk",
    "CREATE TABLE test.fk (id INT PRIMARY KEY, pk_id INT, fk_val STRING)",
    generator::fk_gen,
    10
)

REGISTER_FK(
    "test.fk",
    ("pk_id"),
    "test.pk",
    ("id")
)
