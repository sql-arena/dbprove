include(CMakeParseArguments)

# Function to embed SQL files into a target
function(target_embed_sql TARGET)
    # Parse arguments - everything after the target name is treated as SQL files
    set(SQL_FILES ${ARGN})

    if(NOT SQL_FILES)
        message(FATAL "No SQL files specified for ${TARGET}")
        return()
    endif()

    # Create a unique output directory for this target
    set(OUTPUT_DIR "${CMAKE_BINARY_DIR}/sql_embed/${TARGET}")
    set(OUTPUT_HEADER "${OUTPUT_DIR}/embedded_sql.h")
    file(MAKE_DIRECTORY "${OUTPUT_DIR}")

    # Create target names
    set(GEN_TARGET "${TARGET}_sql_gen")

    # Get the embed script path (in the same directory as this file)
    set(EMBED_SCRIPT "${CMAKE_CURRENT_FUNCTION_LIST_DIR}/embed_sql_files.cmake")

    # Full paths for all SQL files
    set(SQL_FILES_FULL)
    foreach(SQL_FILE ${SQL_FILES})
        if(IS_ABSOLUTE "${SQL_FILE}")
            list(APPEND SQL_FILES_FULL "${SQL_FILE}")
        else()
            list(APPEND SQL_FILES_FULL "${CMAKE_CURRENT_SOURCE_DIR}/${SQL_FILE}")
        endif()
    endforeach()

    # Generate header file on build
    add_custom_command(
            OUTPUT "${OUTPUT_HEADER}"
            COMMAND "${CMAKE_COMMAND}"
            -DOUTPUT=${OUTPUT_HEADER}
            -DSQL_FILES="${SQL_FILES_FULL}"
            -P ${EMBED_SCRIPT}
            DEPENDS ${SQL_FILES_FULL} ${EMBED_SCRIPT}
            COMMENT "Embedding SQL files for target ${TARGET}"
    )

    # Custom target that depends on the header
    add_custom_target(${GEN_TARGET} DEPENDS "${OUTPUT_HEADER}")

    # Add the dependency and includes to the main target
    message(STATUS "Adding target ${OUTPUT_DIR}")

    add_dependencies(${TARGET} ${GEN_TARGET})
    target_include_directories(${TARGET} PRIVATE ${OUTPUT_DIR})

    # Export information about the embedded SQL
    set_property(TARGET ${TARGET} PROPERTY EMBEDDED_SQL_HEADER "${OUTPUT_HEADER}")
    set_property(TARGET ${TARGET} PROPERTY EMBEDDED_SQL_FILES "${SQL_FILES_FULL}")

    message(STATUS "Embedded SQL for target '${TARGET}' - Header: '${OUTPUT_HEADER}'")
endfunction()