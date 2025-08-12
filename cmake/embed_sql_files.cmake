file(READ ${SQL_FILE_LIST_FILE} SQL_FILES)

if(NOT DEFINED SQL_FILES OR SQL_FILES STREQUAL "")
    message(FATAL_ERROR "No SQL files to embed.")
    return()
endif()

message(STATUS "Writing embedded SQL file to ${OUTPUT} from [${SQL_FILES}]")

message(STATUS "Embedding files")
foreach(SQL_FILE IN LISTS SQL_FILES)
    message(STATUS "  ${SQL_FILE}")
endforeach()

file(WRITE ${OUTPUT} "// Auto-generated SQL header\n\n#include <string_view>\n\nnamespace resource {\n\n")

foreach(FILE_PATH IN LISTS SQL_FILES)
    message(STATUS "Processing SQL File: ${FILE_PATH} for embedding")
    get_filename_component(FILE_NAME "${FILE_PATH}" NAME_WE)
    string(REPLACE "." "_" VAR_NAME "${FILE_NAME}")
    set(VAR_NAME "${VAR_NAME}_sql")

    file(READ ${FILE_PATH} CONTENTS)
    string(REPLACE "\\" "\\\\" CONTENTS "${CONTENTS}")
    string(REPLACE "\"" "\\\"" CONTENTS "${CONTENTS}")
    string(REPLACE "\n" "\\n\"\n\"" CONTENTS "${CONTENTS}")

    file(APPEND ${OUTPUT} "constexpr std::string_view ${VAR_NAME} =\n\"${CONTENTS}\";\n\n")
endforeach()

file(APPEND ${OUTPUT} "} // namespace embedded_sql\n")