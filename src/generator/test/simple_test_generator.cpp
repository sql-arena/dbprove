#include <dbprove/generator/generator_state.h>
#include <dbprove/sql/sql.h>
#include <plog/Log.h>

namespace generator {

void pk_gen(GeneratorState& state, sql::ConnectionBase* conn) {
    if (!conn) {
        throw std::runtime_error("Connection required for test.pk generator");
    }
    
    PLOGI << "Generating test.pk using INSERT statements...";
    
    // Create the schema if it doesn't exist (assuming the DDL handled the table)
    // The DDL in REGISTER_GENERATOR will be executed by GeneratorState::load if the table is missing.
    
    for (int i = 1; i <= 5; ++i) {
        std::string stmt = "INSERT INTO test.pk (id, val) VALUES (" + std::to_string(i) + ", 'val_" + std::to_string(i) + "')";
        conn->execute(stmt);
    }
}

void fk_gen(GeneratorState& state, sql::ConnectionBase* conn) {
    if (!conn) {
        throw std::runtime_error("Connection required for test.fk generator");
    }

    PLOGI << "Generating test.fk using INSERT statements...";
    
    for (int i = 1; i <= 10; ++i) {
        int pk_id = (i + 1) / 2;
        std::string stmt = "INSERT INTO test.fk (id, pk_id, fk_val) VALUES (" + 
                           std::to_string(i) + ", " + std::to_string(pk_id) + ", 'fk_val_" + std::to_string(i) + "')";
        conn->execute(stmt);
    }
}

// Register the PK table
REGISTER_GENERATOR(
    "test.pk",
    "CREATE TABLE test.pk (id INT PRIMARY KEY, val STRING)",
    pk_gen,
    5
);

// Register the FK table
REGISTER_GENERATOR(
    "test.fk",
    "CREATE TABLE test.fk (id INT PRIMARY KEY, pk_id INT, fk_val STRING)",
    fk_gen,
    10
);

// Register the Foreign Key relationship
REGISTER_FK(
    "test.fk",
    ("pk_id"),
    "test.pk",
    ("id")
);

} // namespace generator
