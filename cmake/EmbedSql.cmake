include(CMakeParseArguments)

# Function to embed SQL files into a target
function(target_embed_sql TARGET)
    cmake_parse_arguments("ARG" "" "" "SQL_FILES" ${ARGN})

    if(NOT ARG_SQL_FILES)
        message(FATAL "No SQL files specified for ${TARGET}")
    endif()

    # Create a unique output directory for this target
    set(OUTPUT_DIR "${CMAKE_BINARY_DIR}/sql_embed/${TARGET}")
    set(OUTPUT_HEADER "${OUTPUT_DIR}/embedded_sql.h")
    file(MAKE_DIRECTORY "${OUTPUT_DIR}")


    # Get the embed script path (in the same directory as this file)
    set(EMBED_SCRIPT "${CMAKE_CURRENT_FUNCTION_LIST_DIR}/embed_sql_files.cmake")

    # Full paths for all SQL files
    set(SQL_FILES_FULL "$<PATH:ABSOLUTE_PATH,NORMALIZE,${ARG_SQL_FILES},${CMAKE_CURRENT_LIST_DIR}>")

    set(_tmpfile "${CMAKE_CURRENT_BINARY_DIR}/${TARGET}_embed_sql_file_list.txt")
    file(GENERATE OUTPUT ${_tmpfile} CONTENT "${SQL_FILES_FULL}")

    # Generate header file on build
    add_custom_command(
            OUTPUT ${OUTPUT_HEADER}
            COMMAND ${CMAKE_COMMAND}
            -D OUTPUT=${OUTPUT_HEADER}
            -D SQL_FILE_LIST_FILE=${_tmpfile}
            -P ${EMBED_SCRIPT}
            DEPENDS "${SQL_FILES_FULL}" ${EMBED_SCRIPT} ${_tmpfile}
            COMMENT "Embedding SQL files for target ${TARGET}"
            VERBATIM
    )

    target_sources(${TARGET} PRIVATE ${OUTPUT_HEADER})
    target_include_directories(${TARGET} PRIVATE ${OUTPUT_DIR})

    # Export information about the embedded SQL
    set_property(TARGET ${TARGET} PROPERTY EMBEDDED_SQL_HEADER "${OUTPUT_HEADER}")
    set_property(TARGET ${TARGET} PROPERTY EMBEDDED_SQL_FILES "${SQL_FILES_FULL}")

    message(STATUS "Embedded SQL for target '${TARGET}' - Header: '${OUTPUT_HEADER}'")
endfunction()