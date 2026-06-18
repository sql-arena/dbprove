#pragma once
#include "dbprove/generator/generator_state.h"

REGISTER_TABLE(
    "pk",
    "test",
    "CREATE TABLE test.pk (id INT PRIMARY KEY, val STRING)",
    5,
    1
)

REGISTER_TABLE(
    "fk",
    "test",
    "CREATE TABLE test.fk (id INT PRIMARY KEY, pk_id INT, fk_val STRING)",
    10,
    1
)
